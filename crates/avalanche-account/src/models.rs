//! Account-plane model (design §17, M10): users, roles, privileges, grants.
//!
//! Snowflake-shaped subset. Role hierarchies landed in M17.3; ownership-aware
//! data-plane DDL in M17.4 (design §23.7). The only built-in role is
//! `ACCOUNTADMIN`, which bypasses every check.

use serde::{Deserialize, Serialize};

/// Built-in bootstrap role. Members bypass all authorization checks.
pub const ACCOUNTADMIN: &str = "ACCOUNTADMIN";

/// The built-in account every pre-M15.4 record belongs to (design §21.4,
/// M15.4 multi-account tenancy). Journals replayed without an `account`
/// field deserialize into this account, keeping old deployments working.
pub const DEFAULT_ACCOUNT: &str = "embucket";

/// Serde default for record `account` fields — journals written before
/// M15.4 carry no `account` and land in [`DEFAULT_ACCOUNT`].
#[must_use]
pub fn default_account() -> String {
    DEFAULT_ACCOUNT.to_string()
}

/// A top-level account: the tenancy boundary for users, roles, grants,
/// shares and databases (M15.4). Created with `CREATE ACCOUNT` (v1: on one
/// shared account store — same-region trust boundary, §21.1-3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub name: String,
}

/// An account user. `login_name` authenticates over the wire; `name` is the
/// object name referenced by DDL (`CREATE USER`, `GRANT … TO USER`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Owning account (M15.4); defaults to [`DEFAULT_ACCOUNT`] on replay.
    #[serde(default = "default_account")]
    pub account: String,
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
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
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
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
    pub role: String,
    pub privilege: Privilege,
    pub object: AccountObject,
}

/// A user-role link (`GRANT ROLE <role> TO USER <user>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRole {
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
    pub user: String,
    pub role: String,
}

/// A role-role grant (design §23, M17.3 role hierarchies):
/// `GRANT ROLE <child> TO ROLE <parent>` — `child`'s privileges become
/// part of `parent`'s effective privilege set (transitively).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleRole {
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
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
    /// Owning account (M15.4); old events default to [`DEFAULT_ACCOUNT`].
    #[serde(default = "default_account")]
    pub account: String,
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
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
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
    /// Owning account (M15.4).
    #[serde(default = "default_account")]
    pub account: String,
    pub share: String,
    pub privilege: Privilege,
    pub object: AccountObject,
}

/// Replication-group kind (M15.5, design §21.5): `REPLICATION` groups keep
/// replicas read-only forever; `FAILOVER` groups can promote a replica to
/// writable (`ALTER … FAILOVER TO <account>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum GroupKind {
    Replication,
    Failover,
}

/// A replication/failover group (M15.5, design §21.5): a named set of
/// tables in the owning account replicated into every allowed target
/// account's *same-named* database. Zero data copy — refresh is a
/// cross-account metadata clone over the shared Iceberg storage; replica
/// tables are stamped `avalanche.replica` and deny DML until a failover
/// promotion lifts the stamp (failover groups only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationGroup {
    /// Owning (source) account.
    #[serde(default = "default_account")]
    pub account: String,
    pub name: String,
    pub kind: GroupKind,
    /// Allowed target accounts — refresh propagates to each.
    pub accounts: Vec<String>,
    /// Replicated objects (v1: `OBJECT_TYPES = TABLES`, so `Table`
    /// variants only).
    pub tables: Vec<AccountObject>,
    /// Epoch ms of the last refresh (0 = never refreshed).
    #[serde(default)]
    pub last_refresh_ms: u64,
    /// Set by a failover promotion: the promoted replica is writable and
    /// the group refuses further refreshes (design §21.5).
    #[serde(default)]
    pub failed_over: bool,
}