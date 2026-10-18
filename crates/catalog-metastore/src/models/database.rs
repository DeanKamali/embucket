use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::VolumeIdent;

/// A database identifier
pub type DatabaseIdent = String;

/// Account owning databases created before multi-account tenancy (M15.4).
/// Mirrors `avalanche_account::models::DEFAULT_ACCOUNT` (this crate does not
/// depend on `avalanche-account`).
pub const DEFAULT_DATABASE_ACCOUNT: &str = "embucket";

/// Serde default for [`Database::account`]: pre-tenancy journal records
/// replay as belonging to the default account. Mirrors
/// `avalanche_account::models::DEFAULT_ACCOUNT`.
#[must_use]
pub fn default_database_account() -> String {
    DEFAULT_DATABASE_ACCOUNT.to_string()
}

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct Database {
    #[validate(length(min = 1))]
    pub ident: DatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: VolumeIdent,
    pub should_refresh: bool,
    /// Owning account (M15.4). Records for non-default accounts carry a
    /// qualified ident (`{account}/{db}`) in the metastore state.
    #[serde(default = "default_database_account")]
    pub account: String,
}

impl Database {
    #[must_use]
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let db = Database {
            ident: "db".to_string(),
            properties: None,
            volume: "vol".to_string(),
            should_refresh: false,
            account: DEFAULT_DATABASE_ACCOUNT.to_string(),
        };
        assert_eq!(db.prefix("parent"), "parent/db");
    }

    #[test]
    fn legacy_record_without_account_defaults() {
        // Pre-M15.4 journal records have no `account` field.
        let db: Database = serde_json::from_str(
            r#"{"ident":"db","volume":"vol","should_refresh":false}"#,
        )
        .unwrap();
        assert_eq!(db.account, DEFAULT_DATABASE_ACCOUNT);
    }
}
