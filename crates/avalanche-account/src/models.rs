//! Account-plane model (design §17, M10): users, roles, privileges, grants.
//!
//! Snowflake-shaped subset. Role hierarchies and ownership grants are v2;
//! the only built-in role is `ACCOUNTADMIN`, which bypasses every check.

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

/// A share: a named grant bundle attachable across accounts (design §21,
/// M15). The provider grants privileges into the share; a consumer account
/// that mounts the share (M15.2) sees exactly the granted objects.
///
/// v1 kinds: `Outbound` (provider-side, `CREATE SHARE`) and `Inbound`
/// (consumer-side, `CREATE INBOUND SHARE`). Cross-account transport is v2
/// (replication deferred) — an inbound share resolves against the same
/// process's store, the "same-region trust boundary" case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Share {
    pub name: String,
    pub kind: ShareKind,
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