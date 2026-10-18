//! Write-through account journal (design §23, M17.3.1).
//!
//! Bridges the in-memory account store to a durable backend so users,
//! roles, grants and shares survive a crash or restart — the account-plane
//! counterpart of the metastore's `CatalogJournal` (M17.0). The crate
//! carries no backend dependency: embedders (embucketd) adapt their own
//! state store to [`AccountJournal`], keeping the §16.2 crate walls intact.
//!
//! Every [`AccountStore`](crate::store::AccountStore) mutation is mirrored
//! as one [`AccountRecord`] upsert (or delete) under a `domain`/`key` pair;
//! bootstrap replays the journal back into the in-memory state. Records are
//! idempotent per key, so replay order across domains does not matter
//! (unlike the catalog journal, there are no cascading tombstones).

use crate::models::{Account, Grant, LoginEvent, Role, RoleRole, Share, ShareGrant, User, UserRole};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Journal backend errors.
#[derive(Debug, snafu::Snafu)]
pub enum JournalError {
    #[snafu(display("{message}"))]
    Backend { message: String },
}

pub type JournalResult<T> = std::result::Result<T, JournalError>;

/// Logical namespaces the journal stores records under.
pub mod domain {
    pub const ACCOUNTS: &str = "account_accounts";
    pub const USERS: &str = "account_users";
    pub const ROLES: &str = "account_roles";
    pub const USER_ROLES: &str = "account_user_roles";
    pub const ROLE_ROLES: &str = "account_role_roles";
    pub const GRANTS: &str = "account_grants";
    pub const SHARES: &str = "account_shares";
    pub const SHARE_GRANTS: &str = "account_share_grants";
    pub const LOGINS: &str = "account_logins";

    /// All domains (no ordering requirement — records are idempotent per
    /// key; see module docs).
    pub const ALL: [&str; 9] = [
        ACCOUNTS,
        USERS,
        ROLES,
        USER_ROLES,
        ROLE_ROLES,
        GRANTS,
        SHARES,
        SHARE_GRANTS,
        LOGINS,
    ];
}

/// Field separator for composite journal keys (`\u{1}` — never appears in
/// SQL identifiers).
pub(crate) fn key_sep() -> char {
    '\u{1}'
}

/// Account-qualified journal key. The default account keeps the bare
/// (pre-M15.4) key so old journals replay unchanged and old delete
/// markers still hit; a non-default account prefixes the key.
pub(crate) fn account_key(account: &str, base: String) -> String {
    if account == crate::models::DEFAULT_ACCOUNT {
        base
    } else {
        format!(
            "{}{sep}{}",
            account.to_ascii_lowercase(),
            base,
            sep = key_sep()
        )
    }
}

/// The record payloads stored per journal domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum AccountRecord {
    Account(Account),
    User(User),
    Role(Role),
    UserRole(UserRole),
    RoleRole(RoleRole),
    Grant(Grant),
    Share(Share),
    ShareGrant(ShareGrant),
    Login(LoginEvent),
}

impl AccountRecord {
    /// Domain this record is stored under.
    #[must_use]
    pub fn domain(&self) -> &'static str {
        match self {
            Self::Account(_) => domain::ACCOUNTS,
            Self::User(_) => domain::USERS,
            Self::Role(_) => domain::ROLES,
            Self::UserRole(_) => domain::USER_ROLES,
            Self::RoleRole(_) => domain::ROLE_ROLES,
            Self::Grant(_) => domain::GRANTS,
            Self::Share(_) => domain::SHARES,
            Self::ShareGrant(_) => domain::SHARE_GRANTS,
            Self::Login(_) => domain::LOGINS,
        }
    }

    /// Lowercase identity of the record — the journal key. Re-putting a
    /// record with the same key overwrites (idempotent replay); deletes
    /// use the same key. Non-default accounts qualify the key with the
    /// account name (M15.4) so two accounts never collide.
    #[must_use]
    pub fn journal_key(&self) -> String {
        let sep = key_sep();
        match self {
            Self::Account(a) => a.name.to_ascii_lowercase(),
            Self::User(u) => account_key(&u.account, u.name.to_ascii_lowercase()),
            Self::Role(r) => account_key(&r.account, r.name.to_ascii_lowercase()),
            Self::UserRole(ur) => account_key(
                &ur.account,
                format!("{}{sep}{}", ur.user.to_ascii_lowercase(), ur.role.to_ascii_lowercase()),
            ),
            Self::RoleRole(rr) => account_key(
                &rr.account,
                format!("{}{sep}{}", rr.child.to_ascii_lowercase(), rr.parent.to_ascii_lowercase()),
            ),
            Self::Grant(g) => account_key(
                &g.account,
                format!(
                    "{}{sep}{}{sep}{}{sep}{}",
                    g.role.to_ascii_lowercase(),
                    g.privilege.to_string().to_ascii_lowercase(),
                    g.object.kind().to_ascii_lowercase(),
                    g.object.to_string().to_ascii_lowercase()
                ),
            ),
            Self::Share(s) => account_key(&s.account, s.name.to_ascii_lowercase()),
            Self::ShareGrant(g) => account_key(
                &g.account,
                format!(
                    "{}{sep}{}{sep}{}{sep}{}",
                    g.share.to_ascii_lowercase(),
                    g.privilege.to_string().to_ascii_lowercase(),
                    g.object.kind().to_ascii_lowercase(),
                    g.object.to_string().to_ascii_lowercase()
                ),
            ),
            // Timestamp + caller-supplied sequence: unique per event, and
            // the key sorts chronologically (`list_logins` is oldest-first).
            // The account rides in the record for scoped listing.
            Self::Login(e) => format!("{:020}{sep}{:010}", e.timestamp_ms, e.sequence),
        }
    }

    /// Serialize for the journal.
    ///
    /// # Errors
    /// When the record fails to serialize (models are serde-backed, so in
    /// practice this is infallible).
    pub fn to_bytes(&self) -> JournalResult<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| JournalError::Backend {
                message: format!("serialize account record: {e}"),
            })
    }

    /// Deserialize a record previously written under `domain`.
    ///
    /// # Errors
    /// When the bytes do not deserialize into an `AccountRecord`.
    pub fn from_bytes(_domain: &str, value: &[u8]) -> JournalResult<Self> {
        serde_json::from_slice(value).map_err(|e| JournalError::Backend {
            message: format!("deserialize account record: {e}"),
        })
    }
}

/// Durable backend for the account store (write-through + replay).
#[async_trait::async_trait]
pub trait AccountJournal: std::fmt::Debug + Send + Sync {
    /// Upsert one record under `domain`/`key`.
    async fn put(&self, domain: &str, key: &str, value: Vec<u8>) -> JournalResult<()>;

    /// Remove the record stored under `domain`/`key` (no-op if absent).
    async fn delete(&self, domain: &str, key: &str) -> JournalResult<()>;

    /// All `(key, value)` pairs stored under `domain`.
    async fn list(&self, domain: &str) -> JournalResult<Vec<(String, Vec<u8>)>>;
}