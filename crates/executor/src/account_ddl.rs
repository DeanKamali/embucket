//! Pre-parse interception of account-level RBAC DDL (Avalanche design §17,
//! M10). The sqlparser fork has no `GRANT ... ON ACCOUNT OBJECT` grammar, so
//! account statements are matched on the raw SQL before DataFusion parsing.
//! Supported (Snowflake subset, v1):
//!
//! - `CREATE [IF NOT EXISTS] ROLE <name>` / `DROP ROLE <name>`
//! - `CREATE USER <name> PASSWORD = '<pw>' [DEFAULT_ROLE = <r>] [TENANT = '<t>']`
//! - `DROP USER <name>`
//! - `GRANT ROLE <r> TO USER <u>` / `REVOKE ROLE <r> FROM USER <u>`
//! - `GRANT USAGE|SELECT ON DATABASE|SCHEMA|TABLE|WAREHOUSE <name> TO ROLE <r>`
//!   (and the symmetric `REVOKE ... FROM ROLE <r>`)
//! - `SHOW GRANTS TO ROLE <r>`
//! - Shares (design §21, M15.1): `CREATE [IF NOT EXISTS] SHARE <name>`,
//!   `CREATE [IF NOT EXISTS] INBOUND SHARE <name>` (M15.2), `DROP SHARE
//!   <name>`, the same `GRANT|REVOKE ... TO|FROM SHARE <s>`, `SHOW SHARES`,
//!   `DESC SHARE <s>`, `SHOW GRANTS TO SHARE <s>`
//!
//! All account DDL is `ACCOUNTADMIN`-only in v1: the session must be
//! authorized with the built-in role active. Data-plane DDL (CREATE/DROP
//! TABLE/SCHEMA/DATABASE) is NOT enforced in v1 — grant checks happen at
//! resolution time through the session's wrapped catalog providers.

use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use avalanche_account::models::{
    AccountObject, Grant, Privilege, ShareGrant, ShareKind, User, ACCOUNTADMIN,
};
use avalanche_account::store::AccountStore;
use avalanche_account::SessionAuthz;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use snafu::Location;
use snafu::ResultExt;
use std::sync::Arc;

/// A parsed account DDL statement (v1 grammar).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountStatement {
    CreateRole {
        name: String,
        if_not_exists: bool,
    },
    DropRole {
        name: String,
    },
    CreateUser {
        name: String,
        password: String,
        default_role: Option<String>,
        tenant: Option<String>,
        if_not_exists: bool,
    },
    DropUser {
        name: String,
    },
    GrantRole {
        role: String,
        user: String,
    },
    RevokeRole {
        role: String,
        user: String,
    },
    /// `GRANT ROLE <child> TO ROLE <parent>` (M17.3 role hierarchies).
    GrantRoleRole {
        child: String,
        parent: String,
    },
    /// `REVOKE ROLE <child> FROM ROLE <parent>`.
    RevokeRoleRole {
        child: String,
        parent: String,
    },
    Grant {
        privilege: Privilege,
        object: AccountObject,
        role: String,
    },
    Revoke {
        privilege: Privilege,
        object: AccountObject,
        role: String,
    },
    ShowGrants {
        role: String,
    },
    CreateShare {
        kind: ShareKind,
        name: String,
        /// Inbound shares mount a provider-side share (M15.2 `FROM` clause).
        provider: Option<String>,
        if_not_exists: bool,
    },
    DropShare {
        name: String,
    },
    GrantShare {
        privilege: Privilege,
        object: AccountObject,
        share: String,
    },
    RevokeShare {
        privilege: Privilege,
        object: AccountObject,
        share: String,
    },
    ShowShares,
    ShowGrantsShare {
        share: String,
    },
    DescribeShare {
        share: String,
    },
}

/// Lower-case token, quotes and trailing `;` stripped.
fn token(s: &str) -> &str {
    s.trim_end_matches(';')
        .trim_matches(|c| c == '"' || c == '`' || c == '\'')
}

fn normalized(session: &UserSession, name: &str) -> String {
    session
        .ident_normalizer
        .normalize(datafusion::sql::sqlparser::ast::Ident::new(name))
}

fn external_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {err}").into(),
        )),
        location: Location::default(),
    }
}

/// Parse `USAGE|SELECT ON <KIND> <name> TO|FROM <ROLE|SHARE> <grantee>`
/// from `tok[i..]`. `None` = malformed (fall through to the planner's
/// syntax error).
fn parse_on_grant(tok: &[&str], from: usize) -> Option<(Privilege, AccountObject, &'static str, String)> {
    let privilege = match tok.get(from)?.to_ascii_uppercase().as_str() {
        "USAGE" => Privilege::Usage,
        "SELECT" => Privilege::Select,
        _ => return None,
    };
    if !tok.get(from + 1)?.eq_ignore_ascii_case("on") {
        return None;
    }
    let object = match tok.get(from + 2)?.to_ascii_uppercase().as_str() {
        "DATABASE" => AccountObject::Database((*tok.get(from + 3)?).to_string()),
        "SCHEMA" => {
            let parts: Vec<&str> = tok.get(from + 3)?.split('.').collect();
            if parts.len() != 2 {
                return None;
            }
            AccountObject::Schema(parts[0].to_string(), parts[1].to_string())
        }
        "TABLE" => {
            let parts: Vec<&str> = tok.get(from + 3)?.split('.').collect();
            if parts.len() != 3 {
                return None;
            }
            AccountObject::Table(parts[0].to_string(), parts[1].to_string(), parts[2].to_string())
        }
        "WAREHOUSE" => AccountObject::Warehouse(tok.get(from + 3)?.to_string()),
        _ => return None,
    };
    let direction = tok.get(from + 4)?.to_ascii_uppercase();
    if !matches!(direction.as_str(), "TO" | "FROM") {
        return None;
    }
    let grantee_kind = match tok.get(from + 5)?.to_ascii_uppercase().as_str() {
        "ROLE" => "role",
        "SHARE" => "share",
        _ => return None,
    };
    let grantee = (*tok.get(from + 6)?).to_string();
    Some((privilege, object, grantee_kind, grantee))
}

/// Match the grammar prefix; `None` when this is not an account statement
/// (falls through to the normal planner).
#[allow(clippy::too_many_lines)]
pub fn parse(raw: &str) -> Option<AccountStatement> {
    let raw = raw.trim();
    let tok: Vec<&str> = raw.split_whitespace().map(token).collect();
    match tok.first()?.to_ascii_uppercase().as_str() {
        "CREATE" | "DROP" => {}
        "GRANT" | "REVOKE" => {
            let is_grant = tok[0].eq_ignore_ascii_case("grant");
            // Object grant: `GRANT|REVOKE <priv> ON <kind> <name> TO|FROM ROLE|SHARE <g>`
            if let Some((privilege, object, grantee_kind, grantee)) = parse_on_grant(&tok, 1) {
                return Some(match (is_grant, grantee_kind) {
                    (true, "role") => AccountStatement::Grant {
                        privilege,
                        object,
                        role: grantee,
                    },
                    (false, "role") => AccountStatement::Revoke {
                        privilege,
                        object,
                        role: grantee,
                    },
                    (true, "share") => AccountStatement::GrantShare {
                        privilege,
                        object,
                        share: grantee,
                    },
                    (false, "share") => AccountStatement::RevokeShare {
                        privilege,
                        object,
                        share: grantee,
                    },
                    _ => unreachable!("grantee_kind is 'role' or 'share'"),
                });
            }
            // Role membership: `GRANT ROLE <r> TO USER <u>` /
            // `REVOKE ROLE <r> FROM USER <u>`, or the hierarchy form
            // `GRANT ROLE <child> TO ROLE <parent>` (M17.3).
            if tok.len() == 6 && tok[1].eq_ignore_ascii_case("role") {
                let direction_ok =
                    tok[3].eq_ignore_ascii_case(if is_grant { "to" } else { "from" });
                if !direction_ok {
                    return None;
                }
                if tok[4].eq_ignore_ascii_case("user") {
                    let (role, user) = (tok[2].to_string(), tok[5].to_string());
                    return if is_grant {
                        Some(AccountStatement::GrantRole { role, user })
                    } else {
                        Some(AccountStatement::RevokeRole { role, user })
                    };
                }
                if tok[4].eq_ignore_ascii_case("role") {
                    let (child, parent) = (tok[2].to_string(), tok[5].to_string());
                    return if is_grant {
                        Some(AccountStatement::GrantRoleRole { child, parent })
                    } else {
                        Some(AccountStatement::RevokeRoleRole { child, parent })
                    };
                }
            }
            return None;
        }
        "SHOW" => {
            if tok.len() == 5
                && tok[1].eq_ignore_ascii_case("grants")
                && tok[2].eq_ignore_ascii_case("to")
            {
                if tok[3].eq_ignore_ascii_case("role") {
                    return Some(AccountStatement::ShowGrants {
                        role: tok[4].to_string(),
                    });
                }
                if tok[3].eq_ignore_ascii_case("share") {
                    return Some(AccountStatement::ShowGrantsShare {
                        share: tok[4].to_string(),
                    });
                }
                return None;
            }
            if tok.len() == 2 && tok[1].eq_ignore_ascii_case("shares") {
                return Some(AccountStatement::ShowShares);
            }
            return None;
        }
        "DESC" | "DESCRIBE" => {
            if tok.len() == 3 && tok[1].eq_ignore_ascii_case("share") {
                return Some(AccountStatement::DescribeShare {
                    share: tok[2].to_string(),
                });
            }
            return None;
        }
        _ => return None,
    }

    let create = tok[0].eq_ignore_ascii_case("create");
    let mut i = 1;
    // M15.2: `CREATE INBOUND SHARE [IF NOT EXISTS] <name>` — INBOUND SHARE
    // occupies the kind position (two tokens).
    let mut inbound_share = false;
    if tok.get(i).is_some_and(|t| t.eq_ignore_ascii_case("inbound"))
        && tok.get(i + 1).is_some_and(|t| t.eq_ignore_ascii_case("share"))
    {
        inbound_share = true;
        i += 2;
    }
    // Grammar: CREATE [INBOUND SHARE]|ROLE|USER|SHARE <name> [options]
    let kind = if inbound_share {
        // `INBOUND SHARE` already consumed; the next token is the name.
        "SHARE".to_string()
    } else {
        let k = tok.get(i)?.to_ascii_uppercase();
        if k != "ROLE" && k != "USER" && k != "SHARE" {
            return None;
        }
        i += 1;
        k
    };
    let mut if_not_exists = false;
    if create
        && tok.get(i).is_some_and(|t| t.eq_ignore_ascii_case("if"))
        && tok.get(i + 1).is_some_and(|t| t.eq_ignore_ascii_case("not"))
        && tok.get(i + 2).is_some_and(|t| t.eq_ignore_ascii_case("exists"))
    {
        if_not_exists = true;
        i += 3;
    }
    let name = (*tok.get(i)?).to_string();
    if create {
        if kind == "ROLE" || kind == "SHARE" || inbound_share {
            // M15.2: inbound shares take `FROM <provider_share>`.
            let mut provider = None;
            if inbound_share
                && tok
                    .get(i + 1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("from"))
            {
                provider = Some((*tok.get(i + 2)?).to_string());
                if tok.get(i + 3).is_some() {
                    return None;
                }
            } else if tok.get(i + 1).is_some() {
                return None; // unknown CREATE ROLE/SHARE options → planner error
            }
            return if kind == "ROLE" {
                Some(AccountStatement::CreateRole { name, if_not_exists })
            } else if inbound_share {
                // Malformed inbound share (missing FROM) → planner error.
                if provider.is_none() {
                    return None;
                }
                Some(AccountStatement::CreateShare {
                    kind: ShareKind::Inbound,
                    name,
                    provider,
                    if_not_exists,
                })
            } else {
                Some(AccountStatement::CreateShare {
                    kind: ShareKind::Outbound,
                    name,
                    provider: None,
                    if_not_exists,
                })
            };
        }
        // CREATE USER <name> [options]: key = 'value' pairs.
        let mut password = None;
        let mut default_role = None;
        let mut tenant = None;
        let mut j = i + 1;
        while j < tok.len() {
            let key = tok[j].to_ascii_uppercase();
            let Some(value) = tok.get(j + 2) else {
                return None;
            };
            if !tok[j + 1].eq_ignore_ascii_case("=") {
                return None;
            }
            match key.as_str() {
                "PASSWORD" => password = Some(value.to_string()),
                "DEFAULT_ROLE" => default_role = Some(value.to_string()),
                "TENANT" => tenant = Some(value.to_string()),
                _ => return None, // unknown user option → planner error
            }
            j += 3;
        }
        let password = password?;
        return Some(AccountStatement::CreateUser {
            name,
            password,
            default_role,
            tenant,
            if_not_exists,
        });
    }
    // DROP ROLE|USER|SHARE <name>
    if tok.get(i + 1).is_some() {
        return None;
    }
    match kind.as_str() {
        "ROLE" => Some(AccountStatement::DropRole { name }),
        "USER" => Some(AccountStatement::DropUser { name }),
        _ => Some(AccountStatement::DropShare { name }),
    }
}

/// Entry point: run account DDL when the raw SQL matches; `None` = fall
/// through to the normal planner.
pub async fn try_execute(raw: &str, session: &UserSession) -> Option<Result<QueryResult>> {
    let statement = parse(&crate::sql_text::strip_comments(raw))?;
    Some(execute(&statement, session).await)
}

/// Account DDL requires the `ACCOUNTADMIN` role to be active on this session;
/// returns the authz context and the account store.
fn admin_store(session: &UserSession) -> Result<(Arc<SessionAuthz>, Arc<dyn AccountStore>)> {
    let Some(authz) = &session.authz else {
        return Err(external_error(
            "account",
            "insufficient privileges to operate on account-level objects",
        ));
    };
    if !authz.is_admin_now() {
        return Err(external_error(
            "account",
            format!(
                "insufficient privileges to operate on account-level objects (active role '{}')",
                authz.active_role()
            ),
        ));
    }
    let authz = Arc::clone(authz);
    let store = authz.store();
    Ok((authz, store))
}

#[allow(clippy::too_many_lines)]
async fn execute(stmt: &AccountStatement, session: &UserSession) -> Result<QueryResult> {
    let (_authz, store) = admin_store(session)?;
    let norm = |name: &str| normalized(session, name);
    match stmt {
        AccountStatement::CreateRole { name, if_not_exists } => {
            let name = norm(name);
            match store.create_role(&name).await {
                Ok(_) => crate::clone_ddl::status_row(),
                Err(e) if e.is_role_already_exists() && *if_not_exists => {
                    crate::clone_ddl::status_row()
                }
                Err(e) => Err(external_error("account", e)),
            }
        }
        AccountStatement::DropRole { name } => {
            let name = norm(name);
            if name.eq_ignore_ascii_case(ACCOUNTADMIN) {
                return Err(external_error(
                    "account",
                    format!("cannot drop the built-in role '{ACCOUNTADMIN}'"),
                ));
            }
            store.drop_role(&name).await.map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::CreateUser {
            name,
            password,
            default_role,
            tenant,
            if_not_exists,
        } => {
            let name = norm(name);
            let user = User {
                name: name.clone(),
                login_name: name.clone(),
                password: password.to_string(),
                default_role: default_role.as_deref().map(|r| norm(r)),
                tenant: tenant.clone(),
                disabled: false,
            };
            match store.create_user(user).await {
                Ok(_) => crate::clone_ddl::status_row(),
                Err(e) if e.is_user_already_exists() && *if_not_exists => {
                    crate::clone_ddl::status_row()
                }
                Err(e) => Err(external_error("account", e)),
            }
        }
        AccountStatement::DropUser { name } => {
            let name = norm(name);
            if name.eq_ignore_ascii_case(ACCOUNTADMIN)
                || _authz.login_name().eq_ignore_ascii_case(&name)
            {
                return Err(external_error(
                    "account",
                    format!("cannot drop user '{name}'"),
                ));
            }
            store.drop_user(&name).await.map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::GrantRole { role, user } => {
            store
                .grant_role_to_user(&norm(user), &norm(role))
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::RevokeRole { role, user } => {
            store
                .revoke_role_from_user(&norm(user), &norm(role))
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::GrantRoleRole { child, parent } => {
            store
                .grant_role_to_role(&norm(child), &norm(parent))
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::RevokeRoleRole { child, parent } => {
            store
                .revoke_role_from_role(&norm(child), &norm(parent))
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::Grant {
            privilege,
            object,
            role,
        } => {
            let (privilege, object) = normalize_grant(privilege.clone(), object, session);
            store
                .grant(Grant {
                    role: norm(role),
                    privilege,
                    object,
                })
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::Revoke {
            privilege,
            object,
            role,
        } => {
            let (privilege, object) = normalize_grant(privilege.clone(), object, session);
            store
                .revoke(&Grant {
                    role: norm(role),
                    privilege,
                    object,
                })
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::ShowGrants { role } => {
            let role = norm(role);
            if store.get_role(&role).await.map_err(|e| external_error("account", e))?.is_none() {
                return Err(external_error(
                    "account",
                    format!("role '{role}' does not exist"),
                ));
            }
            let grants = store
                .role_grants(&role)
                .await
                .map_err(|e| external_error("account", e))?;
            let mut schema_fields = Vec::new();
            for (name, nullable) in [
                ("privilege", false),
                ("granted_on", false),
                ("name", false),
                ("granted_to", false),
                ("grantee_name", false),
            ] {
                schema_fields.push(Field::new(name, DataType::Utf8, nullable));
            }
            let schema = Arc::new(ArrowSchema::new(schema_fields));
            let mut privilege_col = Vec::new();
            let mut granted_on_col = Vec::new();
            let mut name_col = Vec::new();
            let mut granted_to_col = Vec::new();
            let mut grantee_col = Vec::new();
            for g in grants {
                privilege_col.push(g.privilege.to_string());
                granted_on_col.push(g.object.kind().to_string());
                name_col.push(g.object.to_string());
                granted_to_col.push("ROLE".to_string());
                grantee_col.push(role.clone());
            }
            // Role hierarchies (M17.3): roles granted *to* this role show
            // as `GRANT ROLE <child>` rows (Snowflake shape).
            let children = store
                .role_children(&role)
                .await
                .map_err(|e| external_error("account", e))?;
            for child in children {
                privilege_col.push(format!("GRANT ROLE {child}"));
                granted_on_col.push("ROLE".to_string());
                name_col.push(child);
                granted_to_col.push("ROLE".to_string());
                grantee_col.push(role.clone());
            }
            if privilege_col.is_empty() {
                // Snowflake keeps SHOW GRANTS non-empty; a role with no
                // grants reports one row with empty strings.
                privilege_col.push(String::new());
                granted_on_col.push(String::new());
                name_col.push(String::new());
                granted_to_col.push("ROLE".to_string());
                grantee_col.push(role.clone());
            }
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(privilege_col)),
                    Arc::new(StringArray::from(granted_on_col)),
                    Arc::new(StringArray::from(name_col)),
                    Arc::new(StringArray::from(granted_to_col)),
                    Arc::new(StringArray::from(grantee_col)),
                ],
            )
            .context(ex_error::ArrowSnafu)?;
            Ok(QueryResult::new(vec![batch], schema))
        }
        AccountStatement::CreateShare { kind, name, provider, if_not_exists } => {
            let name = norm(name);
            let result = match kind {
                ShareKind::Outbound => store.create_share(&name).await,
                ShareKind::Inbound => {
                    let Some(provider) = provider else {
                        return Err(external_error(
                            "account",
                            "CREATE INBOUND SHARE requires FROM <provider_share>",
                        ));
                    };
                    store.create_inbound_share(&name, &norm(provider)).await
                }
            };
            match result {
                Ok(_) => crate::clone_ddl::status_row(),
                Err(e) if e.is_share_already_exists() && *if_not_exists => {
                    crate::clone_ddl::status_row()
                }
                Err(e) => Err(external_error("account", e)),
            }
        }
        AccountStatement::DropShare { name } => {
            let name = norm(name);
            store.drop_share(&name).await.map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::GrantShare {
            privilege,
            object,
            share,
        } => {
            let (privilege, object) = normalize_grant(privilege.clone(), object, session);
            store
                .grant_to_share(ShareGrant {
                    share: norm(share),
                    privilege,
                    object,
                })
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::RevokeShare {
            privilege,
            object,
            share,
        } => {
            let (privilege, object) = normalize_grant(privilege.clone(), object, session);
            store
                .revoke_from_share(&ShareGrant {
                    share: norm(share),
                    privilege,
                    object,
                })
                .await
                .map_err(|e| external_error("account", e))?;
            crate::clone_ddl::status_row()
        }
        AccountStatement::ShowShares => {
            let shares = store
                .list_shares()
                .await
                .map_err(|e| external_error("account", e))?;
            let mut schema_fields = Vec::new();
            for (name, nullable) in [("created_on", false), ("name", false), ("kind", false)] {
                schema_fields.push(Field::new(name, DataType::Utf8, nullable));
            }
            let schema = Arc::new(ArrowSchema::new(schema_fields));
            let mut created_on_col = Vec::new();
            let mut name_col = Vec::new();
            let mut kind_col = Vec::new();
            for s in shares {
                // Timestamps are not tracked in the v1 in-memory store.
                created_on_col.push(String::new());
                name_col.push(s.name);
                kind_col.push(s.kind.to_string());
            }
            if created_on_col.is_empty() {
                created_on_col.push(String::new());
                name_col.push(String::new());
                kind_col.push(String::new());
            }
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(created_on_col)),
                    Arc::new(StringArray::from(name_col)),
                    Arc::new(StringArray::from(kind_col)),
                ],
            )
            .context(ex_error::ArrowSnafu)?;
            Ok(QueryResult::new(vec![batch], schema))
        }
        AccountStatement::ShowGrantsShare { share } => {
            let share = norm(share);
            if store
                .get_share(&share)
                .await
                .map_err(|e| external_error("account", e))?
                .is_none()
            {
                return Err(external_error(
                    "account",
                    format!("share '{share}' does not exist"),
                ));
            }
            let grants = if let Some(s) = store
                .get_share(&share)
                .await
                .map_err(|e| external_error("account", e))?
            {
                if let (ShareKind::Inbound, Some(provider)) = (s.kind, s.provider.as_deref()) {
                    // Inbound share: report the provider bundle it mounts.
                    let provider = norm(provider);
                    if store
                        .get_share(&provider)
                        .await
                        .map_err(|e| external_error("account", e))?
                        .is_none()
                    {
                        return Err(external_error(
                            "account",
                            format!("share '{provider}' does not exist"),
                        ));
                    }
                    store
                        .share_grants(&provider)
                        .await
                        .map_err(|e| external_error("account", e))?
                } else {
                    store
                        .share_grants(&share)
                        .await
                        .map_err(|e| external_error("account", e))?
                }
            } else {
                return Err(external_error(
                    "account",
                    format!("share '{share}' does not exist"),
                ));
            };
            let mut schema_fields = Vec::new();
            for (name, nullable) in [
                ("privilege", false),
                ("granted_on", false),
                ("name", false),
                ("granted_to", false),
                ("grantee_name", false),
            ] {
                schema_fields.push(Field::new(name, DataType::Utf8, nullable));
            }
            let schema = Arc::new(ArrowSchema::new(schema_fields));
            let mut privilege_col = Vec::new();
            let mut granted_on_col = Vec::new();
            let mut name_col = Vec::new();
            let mut granted_to_col = Vec::new();
            let mut grantee_col = Vec::new();
            for g in grants {
                privilege_col.push(g.privilege.to_string());
                granted_on_col.push(g.object.kind().to_string());
                name_col.push(g.object.to_string());
                granted_to_col.push("SHARE".to_string());
                grantee_col.push(share.clone());
            }
            if privilege_col.is_empty() {
                // Snowflake keeps SHOW GRANTS non-empty; a share with no
                // grants reports one row with empty strings.
                privilege_col.push(String::new());
                granted_on_col.push(String::new());
                name_col.push(String::new());
                granted_to_col.push("SHARE".to_string());
                grantee_col.push(share.clone());
            }
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(privilege_col)),
                    Arc::new(StringArray::from(granted_on_col)),
                    Arc::new(StringArray::from(name_col)),
                    Arc::new(StringArray::from(granted_to_col)),
                    Arc::new(StringArray::from(grantee_col)),
                ],
            )
            .context(ex_error::ArrowSnafu)?;
            Ok(QueryResult::new(vec![batch], schema))
        }
        AccountStatement::DescribeShare { share } => {
            let share = norm(share);
            let Some(s) = store
                .get_share(&share)
                .await
                .map_err(|e| external_error("account", e))?
            else {
                return Err(external_error(
                    "account",
                    format!("share '{share}' does not exist"),
                ));
            };
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("created_on", DataType::Utf8, false),
                Field::new("kind", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            // Timestamps are not tracked in the v1 in-memory store.
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec![String::new()])),
                    Arc::new(StringArray::from(vec![s.kind.to_string()])),
                    Arc::new(StringArray::from(vec![s.name])),
                ],
            )
            .context(ex_error::ArrowSnafu)?;
            Ok(QueryResult::new(vec![batch], schema))
        }
    }
}

/// Normalize grant object parts through the session ident normalizer.
fn normalize_grant(
    privilege: Privilege,
    object: &AccountObject,
    session: &UserSession,
) -> (Privilege, AccountObject) {
    let object = match object {
        AccountObject::Database(db) => AccountObject::Database(normalized(session, db)),
        AccountObject::Schema(db, s) => {
            AccountObject::Schema(normalized(session, db), normalized(session, s))
        }
        AccountObject::Table(db, s, t) => AccountObject::Table(
            normalized(session, db),
            normalized(session, s),
            normalized(session, t),
        ),
        AccountObject::Warehouse(wh) => AccountObject::Warehouse(normalized(session, wh)),
    };
    (privilege, object)
}

/// Validate `USE ROLE` against the account store (design §17): the session's
/// user must hold the role. Errors are Snowflake-shaped.
pub async fn use_role(session: &UserSession, role: &str) -> Result<()> {
    let Some(authz) = &session.authz else {
        return Ok(()); // no account context (internal session) — unchanged
    };
    let role = normalized(session, role);
    authz.activate_role(&role).await.map_err(|e| {
        external_error(
            "use role",
            format!("role '{role}' does not exist or not authorized ({e})"),
        )
    })
}

/// Validate `USE WAREHOUSE` against the active role's grants.
pub fn use_warehouse(session: &UserSession, warehouse: &str) -> Result<()> {
    let Some(authz) = &session.authz else {
        return Ok(());
    };
    let wh = normalized(session, warehouse);
    if authz.warehouse_usable(&wh) {
        Ok(())
    } else {
        Err(external_error(
            "use warehouse",
            format!("warehouse '{wh}' does not exist or not authorized"),
        ))
    }
}

/// Ensure the session's current warehouse is still usable after rehydration
/// (resume path in `UserQuery::execute`).
pub fn check_current_warehouse(session: &UserSession) -> Result<()> {
    if let Some(authz) = &session.authz {
        if let Some(warehouse) = session.get_session_variable("warehouse") {
            if !authz.warehouse_usable(&warehouse) {
                return Err(external_error(
                    "use warehouse",
                    format!("warehouse '{warehouse}' does not exist or not authorized"),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse, AccountStatement, AccountObject, Privilege, ShareKind};

    fn parsed(sql: &str) -> AccountStatement {
        parse(sql).unwrap_or_else(|| panic!("expected account statement: {sql}"))
    }

    #[test]
    fn parses_create_drop_role() {
        assert_eq!(
            parsed("CREATE ROLE analyst"),
            AccountStatement::CreateRole {
                name: "analyst".into(),
                if_not_exists: false
            }
        );
        assert_eq!(
            parsed("create role if not exists \"Analyst\";"),
            AccountStatement::CreateRole {
                name: "Analyst".into(),
                if_not_exists: true
            }
        );
        assert_eq!(
            parsed("DROP ROLE analyst"),
            AccountStatement::DropRole { name: "analyst".into() }
        );
        assert!(parse("CREATE ROLE").is_none());
        assert!(parse("CREATE TABLE t (a int)").is_none());
    }

    #[test]
    fn parses_create_user_options() {
        assert_eq!(
            parsed("CREATE USER u1 PASSWORD = 'pw' DEFAULT_ROLE = r1 TENANT = 'acme'"),
            AccountStatement::CreateUser {
                name: "u1".into(),
                password: "pw".into(),
                default_role: Some("r1".into()),
                tenant: Some("acme".into()),
                if_not_exists: false
            }
        );
        assert_eq!(
            parsed("CREATE USER IF NOT EXISTS u2 PASSWORD = 'p'"),
            AccountStatement::CreateUser {
                name: "u2".into(),
                password: "p".into(),
                default_role: None,
                tenant: None,
                if_not_exists: true
            }
        );
        assert!(parse("CREATE USER u1").is_none());
        assert!(parse("CREATE USER u1 PASSWORD = 'p' FOO = 'x'").is_none());
    }

    #[test]
    fn parses_grants() {
        assert_eq!(
            parsed("GRANT USAGE ON DATABASE embucket TO ROLE analyst"),
            AccountStatement::Grant {
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
                role: "analyst".into()
            }
        );
        assert_eq!(
            parsed("grant select on table db.public.t to role r1;"),
            AccountStatement::Grant {
                privilege: Privilege::Select,
                object: AccountObject::Table("db".into(), "public".into(), "t".into()),
                role: "r1".into()
            }
        );
        assert_eq!(
            parsed("REVOKE USAGE ON SCHEMA db.public FROM ROLE analyst"),
            AccountStatement::Revoke {
                privilege: Privilege::Usage,
                object: AccountObject::Schema("db".into(), "public".into()),
                role: "analyst".into()
            }
        );
        assert_eq!(
            parsed("GRANT USAGE ON WAREHOUSE wh TO ROLE analyst"),
            AccountStatement::Grant {
                privilege: Privilege::Usage,
                object: AccountObject::Warehouse("wh".into()),
                role: "analyst".into()
            }
        );
        assert_eq!(
            parsed("GRANT ROLE r1 TO USER u1"),
            AccountStatement::GrantRole {
                role: "r1".into(),
                user: "u1".into()
            }
        );
        assert_eq!(
            parsed("REVOKE ROLE r1 FROM USER u1"),
            AccountStatement::RevokeRole {
                role: "r1".into(),
                user: "u1".into()
            }
        );
        assert!(parse("GRANT INSERT ON TABLE db.s.t TO ROLE r").is_none());
        // Role hierarchies (M17.3).
        assert_eq!(
            parsed("GRANT ROLE junior TO ROLE analyst"),
            AccountStatement::GrantRoleRole {
                child: "junior".into(),
                parent: "analyst".into()
            }
        );
        assert_eq!(
            parsed("revoke role junior from role analyst;"),
            AccountStatement::RevokeRoleRole {
                child: "junior".into(),
                parent: "analyst".into()
            }
        );
        assert!(parse("GRANT ROLE r1 TO SHARE s").is_none());
    }

    #[test]
    fn parses_show_grants() {
        assert_eq!(
            parsed("SHOW GRANTS TO ROLE analyst"),
            AccountStatement::ShowGrants { role: "analyst".into() }
        );
        assert!(parse("SHOW GRANTS").is_none());
        assert!(parse("SHOW GRANTS TO USER u1").is_none());
    }

    #[test]
    fn parses_create_drop_share() {
        assert_eq!(
            parsed("CREATE SHARE vendor"),
            AccountStatement::CreateShare {
                kind: ShareKind::Outbound,
                name: "vendor".into(),
                provider: None,
                if_not_exists: false
            }
        );
        assert_eq!(
            parsed("create share \"Vendor\";"),
            AccountStatement::CreateShare {
                kind: ShareKind::Outbound,
                name: "Vendor".into(),
                provider: None,
                if_not_exists: false
            }
        );
        assert_eq!(
            parsed("DROP SHARE vendor"),
            AccountStatement::DropShare { name: "vendor".into() }
        );
        assert!(parse("CREATE SHARE").is_none());
        // Unknown options fall through to the planner.
        assert!(parse("CREATE SHARE vendor FOO = 'x'").is_none());
    }

    #[test]
    fn parses_inbound_share_and_desc() {
        assert_eq!(
            parsed("CREATE INBOUND SHARE acme_in FROM vendor"),
            AccountStatement::CreateShare {
                kind: ShareKind::Inbound,
                name: "acme_in".into(),
                provider: Some("vendor".into()),
                if_not_exists: false
            }
        );
        assert_eq!(
            parsed("CREATE INBOUND SHARE IF NOT EXISTS \"AcmeIn\" FROM \"Vendor\";"),
            AccountStatement::CreateShare {
                kind: ShareKind::Inbound,
                name: "AcmeIn".into(),
                provider: Some("Vendor".into()),
                if_not_exists: true
            }
        );
        // FROM is required for inbound shares.
        assert!(parse("CREATE INBOUND SHARE acme_in").is_none());
        assert!(parse("CREATE INBOUND SHARE acme_in FROM v EXTRA").is_none());
        assert!(parse("CREATE INBOUND ROLE x").is_none());
        // DROP INBOUND SHARE is an accepted alias of DROP SHARE.
        assert_eq!(
            parsed("DROP INBOUND SHARE x"),
            AccountStatement::DropShare { name: "x".into() }
        );
        assert_eq!(
            parsed("DESC SHARE vendor"),
            AccountStatement::DescribeShare { share: "vendor".into() }
        );
        assert_eq!(
            parsed("DESCRIBE SHARE vendor;"),
            AccountStatement::DescribeShare { share: "vendor".into() }
        );
        assert!(parse("DESC TABLE t").is_none());
    }

    #[test]
    fn parses_share_grants_and_shows() {
        assert_eq!(
            parsed("GRANT USAGE ON DATABASE embucket TO SHARE vendor"),
            AccountStatement::GrantShare {
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
                share: "vendor".into()
            }
        );
        assert_eq!(
            parsed("grant select on table db.public.t to share vendor;"),
            AccountStatement::GrantShare {
                privilege: Privilege::Select,
                object: AccountObject::Table("db".into(), "public".into(), "t".into()),
                share: "vendor".into()
            }
        );
        assert_eq!(
            parsed("REVOKE USAGE ON SCHEMA db.public FROM SHARE vendor"),
            AccountStatement::RevokeShare {
                privilege: Privilege::Usage,
                object: AccountObject::Schema("db".into(), "public".into()),
                share: "vendor".into()
            }
        );
        assert_eq!(
            parsed("SHOW GRANTS TO SHARE vendor"),
            AccountStatement::ShowGrantsShare { share: "vendor".into() }
        );
        assert_eq!(parsed("SHOW SHARES"), AccountStatement::ShowShares);
        // Role grants still parse as roles, not shares.
        assert_eq!(
            parsed("GRANT USAGE ON DATABASE embucket TO ROLE analyst"),
            AccountStatement::Grant {
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
                role: "analyst".into()
            }
        );
        assert!(parse("SHOW SHARES ALL").is_none());
    }
}