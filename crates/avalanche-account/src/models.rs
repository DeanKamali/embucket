//! Account-plane model (design §17, M10): users, roles, privileges, grants.
//!
//! Snowflake-shaped subset. Role hierarchies landed in M17.3; ownership-aware
//! data-plane DDL in M17.4 (design §23.7). The only built-in role is
//! `ACCOUNTADMIN`, which bypasses every check.

use serde::{Deserialize, Serialize};

/// Built-in bootstrap role. Members bypass all authorization checks.
pub const ACCOUNTADMIN: &str = "ACCOUNTADMIN";

/// An account user. `login_name` authenticates over the wire; `name` is the
/// object name referenced by DDL (`CREATE USER`, `GRANT … TO USER`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub login_name: String,
    pub password: String,
    pub default_role: Option<String>,
    /// Tenant label (v1, advisory — see design §17). Users with the same
    /// label share a tenancy boundary.
    pub tenant: Option<String>,
    pub disabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Privilege {
    /// USAGE on a database, schema or warehouse.
    Usage,
    /// SELECT on a table.
    Select,
    /// CREATE on a database (create schemas) or schema (create tables)
    /// — design §23.7, ownership-aware data-plane DDL.
    Create,
    /// OWNERSHIP on a database, schema or table: the creator role holds it
    /// implicitly; DROP and children-creation derive from it.
    Ownership,
}

/// The four grantable object kinds (design §17).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccountObject {
    Database(String),
    Schema(String, String),
    Table(String, String, String),
    Warehouse(String),
}

impl AccountObject {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Database(_) => "DATABASE",
            Self::Schema(_, _) => "SCHEMA",
            Self::Table(_, _, _) => "TABLE",
            Self::Warehouse(_) => "WAREHOUSE",
        }
    }

    /// The database this object lives in (itself for `Database`).
    #[must_use]
    pub fn database(&self) -> &str {
        match self {
            Self::Database(db) | Self::Schema(db, _) | Self::Table(db, _, _) => db,
            Self::Warehouse(_) => "",
        }
    }

    /// The object itself followed by its ancestor containers, innermost
    /// first — ownership of any container covers the object (design
    /// §23.7, ownership-aware data-plane DDL).
    #[must_use]
    pub fn with_containers(&self) -> Vec<Self> {
        match self {
            Self::Database(_) | Self::Warehouse(_) => vec![self.clone()],
            Self::Schema(db, s) => vec![
                Self::Schema(db.clone(), s.clone()),
                Self::Database(db.clone()),
            ],
            Self::Table(db, s, t) => vec![
                Self::Table(db.clone(), s.clone(), t.clone()),
                Self::Schema(db.clone(), s.clone()),
                Self::Database(db.clone()),
            ],
        }
    }
}

impl std::fmt::Display for AccountObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(db) => write!(f, "{db}"),
            Self::Schema(db, schema) => write!(f, "{db}.{schema}"),
            Self::Table(db, schema, table) => write!(f, "{db}.{schema}.{table}"),
            Self::Warehouse(wh) => write!(f, "{wh}"),
        }
    }
}

/// A privilege grant: `role` may exercise `privilege` on `object`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub role: String,
    pub privilege: Privilege,
    pub object: AccountObject,
}

/// A user-role link (`GRANT ROLE <role> TO USER <user>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRole {
    pub user: String,
    pub role: String,
}

/// A role-role grant (design §23, M17.3 role hierarchies):
/// `GRANT ROLE <child> TO ROLE <parent>` — `child`'s privileges become
/// part of `parent`'s effective privilege set (transitively).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleRole {
    /// The granted (child) role.
    pub child: String,
    /// The receiving (parent) role.
    pub parent: String,
}

/// A login attempt (M17.1, `LOGIN_HISTORY`). Timestamps are epoch
/// milliseconds (the account crate carries no chrono dep).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginEvent {
    pub timestamp_ms: u64,
    /// Process-local insertion sequence — gives login journal keys
    /// stability and `list_logins` a total order across restarts
    /// (M17.3.1). Events recorded before the field existed default to 0.
    #[serde(default)]
    pub sequence: u64,
    pub user_name: String,
    pub client_app_id: Option<String>,
    pub client_ip: Option<String>,
    /// `true` for successful authentication, `false` for failures.
    pub success: bool,
}

/// A share: a named grant bundle attachable across accounts (design §21,
/// M15). The provider grants privileges into the share; a consumer account
/// that mounts the share (M15.2) sees exactly the granted objects.
///
/// v1 kinds: `Outbound` (provider-side, `CREATE SHARE`) and `Inbound`
/// (consumer-side, `CREATE INBOUND SHARE … FROM <provider_share>`). An
/// inbound share resolves its grant list through `provider` — v1 resolves
/// against the same process's store (the "same-region trust boundary"
/// case); cross-account transport is v2 (replication deferred).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Share {
    pub name: String,
    pub kind: ShareKind,
    /// The provider-side share an inbound share mounts (None for outbound).
    pub provider: Option<String>,
}

/// Outbound = created by the provider (`CREATE SHARE`); inbound = mounted
/// by the consumer (`CREATE INBOUND SHARE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ShareKind {
    Outbound,
    Inbound,
}

impl Default for ShareKind {
    fn default() -> Self {
        Self::Outbound
    }
}

/// A privilege grant to a share: `share` may exercise `privilege` on
/// `object`. Same privilege/object vocabulary as [`Grant`] — no new
/// enforcement path, only a new grantee kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareGrant {
    pub share: String,
    pub privilege: Privilege,
    pub object: AccountObject,
}