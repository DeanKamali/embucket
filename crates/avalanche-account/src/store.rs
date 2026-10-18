//! Account store (design §17, M10.1): users, roles, grants.
//!
//! M15.4 multi-account tenancy (design §21.4): every record belongs to an
//! [`Account`](crate::models::Account). The raw store operates on the
//! built-in [`crate::models::DEFAULT_ACCOUNT`] (back-compat with pre-M15.4
//! journals); sessions use a [`ScopedAccountStore`] view that stamps and
//! filters by the session's account. `InMemoryAccountStore` keeps
//! process-lifetime state; with a journal (M17.3.1) it is also durable —
//! every mutation is mirrored write-through and bootstrap replays the
//! journal.

use crate::error::{self as account_error, Result};
use crate::models::{
    Account, AccountObject, Grant, LoginEvent, Privilege, Role, RoleRole, Share, ShareGrant, User,
    UserRole, ACCOUNTADMIN, DEFAULT_ACCOUNT,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Process-wide account-state epoch (M17.3.1): bumped on every
/// authorization-relevant mutation (roles, users, role/user links, grants,
/// shares). The executor result-cache fingerprint folds this in so a
/// GRANT/REVOKE between two identical SELECTs is never served a stale
/// cached result (design §19.1). Login recording is deliberately excluded:
/// it cannot change what a role may see.
static ACCOUNT_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Current account-state epoch (see [`ACCOUNT_EPOCH`]).
#[must_use]
pub fn account_epoch() -> u64 {
    ACCOUNT_EPOCH.load(Ordering::Relaxed)
}

fn bump_account_epoch() {
    ACCOUNT_EPOCH.fetch_add(1, Ordering::Relaxed);
}

/// Whether `target` is reachable from `start` walking the children graph
/// (`parent → child` edges of `role_roles`). Keys are pre-lowercased.
fn role_graph_reachable(links: &[RoleRole], start: &str, target: &str) -> bool {
    let mut queue = vec![start.to_string()];
    let mut seen = vec![start.to_string()];
    while let Some(next) = queue.pop() {
        for rr in links.iter().filter(|rr| key(&rr.parent) == next) {
            let ck = key(&rr.child);
            if ck == target {
                return true;
            }
            if !seen.contains(&ck) {
                seen.push(ck.clone());
                queue.push(ck);
            }
        }
    }
    false
}

/// Whether a stored grant's object is `dropped` itself or contained
/// within it (a grant on `db.s` or `db.s.t` dies with `db`; a grant on
/// `db.s.t` dies with `db.s` — design §23.7: dropping an object cascades
/// grant removal to its children).
fn grant_covers_object(grant_obj: &AccountObject, dropped: &AccountObject) -> bool {
    match (grant_obj, dropped) {
        (AccountObject::Schema(gdb, _), AccountObject::Database(tdb)) => key(gdb) == key(tdb),
        (AccountObject::Table(gdb, _, _), AccountObject::Database(tdb)) => key(gdb) == key(tdb),
        (AccountObject::Table(gdb, gs, _), AccountObject::Schema(tdb, ts)) => {
            key(gdb) == key(tdb) && key(gs) == key(ts)
        }
        _ => grant_obj == dropped,
    }
}

#[async_trait]
pub trait AccountStore: std::fmt::Debug + Send + Sync {
    async fn create_role(&self, name: &str) -> Result<Role>;
    async fn drop_role(&self, name: &str) -> Result<()>;
    async fn get_role(&self, name: &str) -> Result<Option<Role>>;
    async fn list_roles(&self) -> Result<Vec<Role>>;

    async fn create_user(&self, user: User) -> Result<User>;
    async fn drop_user(&self, name: &str) -> Result<()>;
    /// Lookup by login name (falls back to the object name).
    async fn get_user(&self, login_name: &str) -> Result<Option<User>>;
    async fn list_users(&self) -> Result<Vec<User>>;

    async fn grant_role_to_user(&self, user: &str, role: &str) -> Result<()>;
    async fn revoke_role_from_user(&self, user: &str, role: &str) -> Result<()>;
    async fn user_roles(&self, user: &str) -> Result<Vec<String>>;

    /// Grant `child` to `parent` (design §23, M17.3 role hierarchies):
    /// `parent`'s effective privilege set becomes the union of its own
    /// grants and every role granted beneath it, transitively. Rejects
    /// self-grants and grants that would create a cycle.
    async fn grant_role_to_role(&self, child: &str, parent: &str) -> Result<()>;
    async fn revoke_role_from_role(&self, child: &str, parent: &str) -> Result<()>;
    /// Roles granted directly *beneath* `parent` (its direct children).
    async fn role_children(&self, parent: &str) -> Result<Vec<String>>;
    /// Transitive closure of `roles` over the role-role graph: each role
    /// plus everything granted beneath it. Cycle-safe (the graph is
    /// acyclic by construction, but the walk defends anyway).
    async fn effective_roles(&self, roles: &[String]) -> Result<Vec<String>>;

    async fn grant(&self, grant: Grant) -> Result<()>;
    async fn revoke(&self, grant: &Grant) -> Result<()>;
    async fn role_grants(&self, role: &str) -> Result<Vec<Grant>>;

    /// Every privilege grant on `object` — and on anything *contained
    /// within it* (a database grant covers its schemas and tables, a
    /// schema grant covers its tables) — regardless of role. Used by
    /// data-plane DDL to remove a dropped object's grants (design §23.7:
    /// Snowflake removes grants when an object is dropped, cascading to
    /// children).
    async fn object_grants(&self, object: &AccountObject) -> Result<Vec<Grant>>;

    /// Shares (design §21, M15.1): named grant bundles.
    async fn create_share(&self, name: &str) -> Result<Share>;
    /// Inbound share mounting the provider-side share `provider` (M15.2).
    /// v1 resolves `provider` in the same store.
    async fn create_inbound_share(&self, name: &str, provider: &str) -> Result<Share>;
    async fn drop_share(&self, name: &str) -> Result<()>;
    async fn get_share(&self, name: &str) -> Result<Option<Share>>;
    async fn list_shares(&self) -> Result<Vec<Share>>;
    async fn grant_to_share(&self, grant: ShareGrant) -> Result<()>;
    async fn revoke_from_share(&self, grant: &ShareGrant) -> Result<()>;
    async fn share_grants(&self, share: &str) -> Result<Vec<ShareGrant>>;

    /// Accounts (M15.4, design §21.4): org-level CRUD. Creating an account
    /// seeds its `ACCOUNTADMIN` role and the initial admin user
    /// (`ADMIN_NAME`/`ADMIN_PASSWORD`); dropping one cascades every record
    /// it owns. The built-in default account cannot be dropped.
    async fn create_account(
        &self,
        name: &str,
        admin_name: &str,
        admin_password: &str,
    ) -> Result<Account>;
    async fn drop_account(&self, name: &str) -> Result<()>;
    async fn list_accounts(&self) -> Result<Vec<Account>>;
    async fn get_account(&self, name: &str) -> Result<Option<Account>>;

    /// A session-scoped view of `account` (M15.4): users, roles, grants,
    /// shares and logins are stamped with and filtered by the account.
    /// `None` when the account does not exist. Org-level operations
    /// (account CRUD) still pass through. Sessions must never see records
    /// of another account through this view.
    async fn account_scope(self: Arc<Self>, account: &str) -> Result<Option<Arc<dyn AccountStore>>>;

    /// Verify login credentials. `Ok(None)` = unknown login or wrong
    /// password (indistinguishable, no existence leak); `Err` is reserved
    /// for store failures. Disabled users also return `Ok(None)`.
    async fn authenticate(&self, login_name: &str, password: &str) -> Result<Option<User>>;

    /// Record a login attempt (M17.1, `LOGIN_HISTORY`). Best-effort at the
    /// call site — never blocks authentication.
    async fn record_login(&self, event: LoginEvent) -> Result<()>;

    /// Every recorded login attempt, oldest first (M17.1).
    async fn list_logins(&self) -> Result<Vec<LoginEvent>>;

    /// Whether `role` may exercise `privilege` on `object`. `ACCOUNTADMIN`
    /// members bypass everything; this method does not encode that — see
    /// [`SessionAuthz`].
    async fn allowed(&self, role: &str, privilege: Privilege, object: &AccountObject)
    -> Result<bool>;
}

#[derive(Debug, Default)]
struct StoreState {
    /// Top-level accounts (M15.4), keyed by lowercase name. The default
    /// account is implicit — it exists even with no record (pre-M15.4
    /// deployments carry none).
    accounts: HashMap<String, Account>,
    users: HashMap<String, User>,
    roles: HashMap<String, Role>,
    grants: Vec<Grant>,
    user_roles: Vec<UserRole>,
    role_roles: Vec<RoleRole>,
    shares: HashMap<String, Share>,
    share_grants: Vec<ShareGrant>,
    logins: Vec<LoginEvent>,
}

/// In-memory [`AccountStore`] over the default account (raw, org-level
/// view). Sessions receive [`ScopedAccountStore`] views instead.
#[derive(Debug, Default)]
pub struct InMemoryAccountStore {
    state: Arc<RwLock<StoreState>>,
    /// Write-through journal (M17.3.1): every mutation is mirrored to the
    /// durable backend before the in-memory state changes; `None` for the
    /// plain process-lifetime store.
    journal: Option<Arc<dyn crate::journal::AccountJournal>>,
    /// Insertion sequence for login events (stable journal keys + ordering).
    login_seq: Arc<AtomicU64>,
}

/// A session-scoped [`AccountStore`] view of one account (M15.4, design
/// §21.4). Reads filter to the account; writes stamp it. Org-level account
/// CRUD passes through to the raw store.
#[derive(Debug, Clone)]
pub struct ScopedAccountStore {
    inner: Arc<InMemoryAccountStore>,
    account: String,
}

impl InMemoryAccountStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Durable store (M17.3.1, design §23): replays the journal into the
    /// in-memory state, then serves every subsequent mutation write-through.
    ///
    /// # Errors
    /// When the backend fails while loading a domain, or a replayed record
    /// fails to deserialize.
    pub async fn new_with_journal(
        journal: Arc<dyn crate::journal::AccountJournal>,
    ) -> Result<Self> {
        let mut state = StoreState::default();
        let mut logins: Vec<LoginEvent> = Vec::new();
        for domain in crate::journal::domain::ALL {
            let entries = journal.list(domain).await.map_err(|e| {
                account_error::InvalidInputSnafu {
                    message: format!("account journal load {domain}: {e}"),
                }
                .build()
            })?;
            for (_, value) in entries {
                let record = crate::journal::AccountRecord::from_bytes(domain, &value)
                    .map_err(|e| {
                        account_error::InvalidInputSnafu {
                            message: format!("account journal replay {domain}: {e}"),
                        }
                        .build()
                    })?;
                match record {
                    crate::journal::AccountRecord::Account(a) => {
                        state.accounts.insert(key(&a.name), a);
                    }
                    crate::journal::AccountRecord::User(u) => {
                        state.users.insert(akey(&u.account, &u.name), u);
                    }
                    crate::journal::AccountRecord::Role(r) => {
                        state.roles.insert(akey(&r.account, &r.name), r);
                    }
                    crate::journal::AccountRecord::UserRole(ur) => {
                        state.user_roles.push(ur);
                    }
                    crate::journal::AccountRecord::RoleRole(rr) => {
                        state.role_roles.push(rr);
                    }
                    crate::journal::AccountRecord::Grant(g) => state.grants.push(g),
                    crate::journal::AccountRecord::Share(s) => {
                        state.shares.insert(akey(&s.account, &s.name), s);
                    }
                    crate::journal::AccountRecord::ShareGrant(g) => state.share_grants.push(g),
                    crate::journal::AccountRecord::Login(e) => logins.push(e),
                }
            }
        }
        // Oldest first: timestamp, then insertion sequence (see
        // `AccountRecord::journal_key` for the login key layout).
        logins.sort_by(|a, b| {
            (a.timestamp_ms, a.sequence).cmp(&(b.timestamp_ms, b.sequence))
        });
        state.logins = logins;
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            journal: Some(journal),
            login_seq: Arc::new(AtomicU64::new(0)),
        })
    }

    /// A scoped view of one account (M15.4). The default account always
    /// resolves (pre-M15.4 records are implicitly default-owned); other
    /// accounts must exist.
    #[must_use]
    pub fn scope(self: &Arc<Self>, account: &str) -> Option<ScopedAccountStore> {
        let exists = account.eq_ignore_ascii_case(DEFAULT_ACCOUNT)
            || try_lock_read_noio(&self.state)
                .map(|s| s.accounts.contains_key(&key(account)))
                .unwrap_or(false);
        // Fall back to an async check when the lock is contended (the
        // sync fast path is only an optimization).
        if exists {
            Some(ScopedAccountStore {
                inner: Arc::clone(self),
                account: account.to_string(),
            })
        } else {
            None
        }
    }

    /// Idempotently seed the built-in `ACCOUNTADMIN` role and a member user
    /// (the demo credentials become the admin login) on a store that may
    /// already carry replayed state — the durable counterpart of
    /// [`InMemoryAccountStore::with_admin`]. No-ops (without journal
    /// writes) when the user already exists.
    ///
    /// # Errors
    /// Forwarded from the store mutations (existence checks pass first, so
    /// in practice only journal/backend failures surface).
    pub async fn ensure_admin(&self, name: &str, password: &str) -> Result<()> {
        if self.get_user(name).await?.is_some() {
            return Ok(());
        }
        if self.get_role(ACCOUNTADMIN).await?.is_none() {
            self.create_role(ACCOUNTADMIN).await?;
        }
        self.create_user(User {
            account: DEFAULT_ACCOUNT.to_string(),
            name: name.to_string(),
            login_name: name.to_string(),
            password: password.to_string(),
            default_role: Some(ACCOUNTADMIN.to_string()),
            tenant: None,
            disabled: false,
        })
        .await?;
        self.grant_role_to_user(name, ACCOUNTADMIN).await
    }

    /// Mirror one record to the journal.
    async fn journal_put(&self, record: &crate::journal::AccountRecord) -> Result<()> {
        if let Some(j) = &self.journal {
            let value = record.to_bytes().map_err(|e| {
                account_error::InvalidInputSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;
            j.put(record.domain(), &record.journal_key(), value.to_vec())
                .await
                .map_err(|e| {
                    account_error::InvalidInputSnafu {
                        message: format!("account journal write: {e}"),
                    }
                    .build()
                })?;
        }
        Ok(())
    }

    /// Mirror one deletion to the journal (key computed from `record`).
    async fn journal_delete(&self, record: &crate::journal::AccountRecord) -> Result<()> {
        if let Some(j) = &self.journal {
            j.delete(record.domain(), &record.journal_key())
                .await
                .map_err(|e| {
                    account_error::InvalidInputSnafu {
                        message: format!("account journal delete: {e}"),
                    }
                    .build()
                })?;
        }
        Ok(())
    }

    /// Seed the built-in `ACCOUNTADMIN` role and a member user (the demo
    /// credentials become the admin login). Consumes and returns the store;
    /// must be called on a fresh store — pre-existing state is discarded.
    /// Every pre-M10 deployment path keeps working: the admin user is the
    /// existing `AUTH_DEMO_USER`.
    #[must_use]
    pub fn with_admin(self, name: &str, password: &str) -> Self {
        let role = Role {
            account: DEFAULT_ACCOUNT.to_string(),
            name: ACCOUNTADMIN.to_string(),
        };
        let user = User {
            account: DEFAULT_ACCOUNT.to_string(),
            name: name.to_string(),
            login_name: name.to_string(),
            password: password.to_string(),
            default_role: Some(ACCOUNTADMIN.to_string()),
            tenant: None,
            disabled: false,
        };
        let mut state = StoreState::default();
        state.roles.insert(akey(&role.account, &role.name), role);
        state.users.insert(akey(&user.account, &user.name), user);
        state.user_roles.push(UserRole {
            account: DEFAULT_ACCOUNT.to_string(),
            user: name.to_string(),
            role: ACCOUNTADMIN.to_string(),
        });
        Self {
            state: Arc::new(RwLock::new(state)),
            journal: None,
            login_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    // ================= per-account core (M15.4) =================
    // Each operation takes the owning account explicitly; the raw trait
    // impl passes DEFAULT_ACCOUNT and ScopedAccountStore passes its own.

    async fn create_role_in(&self, account: &str, name: &str) -> Result<Role> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let role = Role {
            account: account.to_string(),
            name: name.to_string(),
        };
        if state.roles.contains_key(&akey(account, name)) {
            return account_error::RoleAlreadyExistsSnafu { role: name.to_string() }.fail();
        }
        state.roles.insert(akey(account, name), role.clone());
        self.journal_put(&crate::journal::AccountRecord::Role(role.clone()))
            .await?;
        Ok(role)
    }

    async fn drop_role_in(&self, account: &str, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.roles.remove(&akey(account, name)).is_none() {
            return account_error::RoleNotFoundSnafu { role: name.to_string() }.fail();
        }
        // Journal the cascades before mutating (keys computed from the
        // records being removed).
        let removed_grants: Vec<Grant> = state
            .grants
            .iter()
            .filter(|g| g.account.eq_ignore_ascii_case(account))
            .filter(|g| key(&g.role).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        let removed_links: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| ur.account.eq_ignore_ascii_case(account))
            .filter(|ur| key(&ur.role).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        // Role-role grants vanish with either endpoint (M17.3).
        let removed_role_links: Vec<RoleRole> = state
            .role_roles
            .iter()
            .filter(|rr| rr.account.eq_ignore_ascii_case(account))
            .filter(|rr| key(&rr.child) == key(name) || key(&rr.parent) == key(name))
            .cloned()
            .collect();
        for g in &removed_grants {
            self.journal_delete(&crate::journal::AccountRecord::Grant(g.clone()))
                .await?;
        }
        for ur in &removed_links {
            self.journal_delete(&crate::journal::AccountRecord::UserRole(ur.clone()))
                .await?;
        }
        for rr in &removed_role_links {
            self.journal_delete(&crate::journal::AccountRecord::RoleRole(rr.clone()))
                .await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::Role(Role {
            account: account.to_string(),
            name: name.to_string(),
        }))
        .await?;
        state.grants.retain(|g| {
            !(g.account.eq_ignore_ascii_case(account)
                && key(&g.role).eq_ignore_ascii_case(&key(name)))
        });
        state.user_roles.retain(|ur| {
            !(ur.account.eq_ignore_ascii_case(account)
                && key(&ur.role).eq_ignore_ascii_case(&key(name)))
        });
        state.role_roles.retain(|rr| {
            !(rr.account.eq_ignore_ascii_case(account)
                && (key(&rr.child) == key(name) || key(&rr.parent) == key(name)))
        });
        Ok(())
    }

    async fn get_role_in(&self, account: &str, name: &str) -> Result<Option<Role>> {
        let state = self.state.read().await;
        Ok(state.roles.get(&akey(account, name)).cloned())
    }

    async fn list_roles_in(&self, account: &str) -> Result<Vec<Role>> {
        let state = self.state.read().await;
        Ok(state
            .roles
            .values()
            .filter(|r| r.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect())
    }

    async fn create_user_in(&self, account: &str, user: User) -> Result<User> {
        bump_account_epoch();
        let mut user = user;
        user.account = account.to_string();
        let mut state = self.state.write().await;
        if state.users.contains_key(&akey(account, &user.name)) {
            return account_error::UserAlreadyExistsSnafu { user: user.name.clone() }.fail();
        }
        state.users.insert(akey(account, &user.name), user.clone());
        self.journal_put(&crate::journal::AccountRecord::User(user.clone()))
            .await?;
        Ok(user)
    }

    async fn drop_user_in(&self, account: &str, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.users.remove(&akey(account, name)).is_none() {
            return account_error::UserNotFoundSnafu { user: name.to_string() }.fail();
        }
        let removed_links: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| ur.account.eq_ignore_ascii_case(account))
            .filter(|ur| key(&ur.user).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        for ur in &removed_links {
            self.journal_delete(&crate::journal::AccountRecord::UserRole(ur.clone()))
                .await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::User(User {
            account: account.to_string(),
            name: name.to_string(),
            login_name: String::new(),
            password: String::new(),
            default_role: None,
            tenant: None,
            disabled: false,
        }))
        .await?;
        state.user_roles.retain(|ur| {
            !(ur.account.eq_ignore_ascii_case(account)
                && key(&ur.user).eq_ignore_ascii_case(&key(name)))
        });
        Ok(())
    }

    async fn get_user_in(&self, account: &str, login_name: &str) -> Result<Option<User>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .filter(|u| u.account.eq_ignore_ascii_case(account))
            .find(|u| key(&u.login_name) == key(login_name))
            .cloned())
    }

    async fn list_users_in(&self, account: &str) -> Result<Vec<User>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .filter(|u| u.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect())
    }

    async fn grant_role_to_user_in(&self, account: &str, user: &str, role: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.users.contains_key(&akey(account, user)) {
            return account_error::UserNotFoundSnafu { user: user.to_string() }.fail();
        }
        if !state.roles.contains_key(&akey(account, role)) {
            return account_error::RoleNotFoundSnafu { role: role.to_string() }.fail();
        }
        let link = UserRole {
            account: account.to_string(),
            user: user.to_string(),
            role: role.to_string(),
        };
        let exists = state.user_roles.iter().any(|ur| {
            ur.account.eq_ignore_ascii_case(account)
                && key(&ur.user) == key(user)
                && key(&ur.role) == key(role)
        });
        if !exists {
            state.user_roles.push(link.clone());
            self.journal_put(&crate::journal::AccountRecord::UserRole(link))
                .await?;
        }
        Ok(())
    }

    async fn revoke_role_from_user_in(&self, account: &str, user: &str, role: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| {
                ur.account.eq_ignore_ascii_case(account)
                    && key(&ur.user) == key(user)
                    && key(&ur.role) == key(role)
            })
            .cloned()
            .collect();
        if removed.is_empty() {
            return account_error::GrantNotFoundSnafu {
                message: format!("role '{role}' is not granted to user '{user}'"),
            }
            .fail();
        }
        for ur in &removed {
            self.journal_delete(&crate::journal::AccountRecord::UserRole(ur.clone()))
                .await?;
        }
        state.user_roles.retain(|ur| {
            !(ur.account.eq_ignore_ascii_case(account)
                && key(&ur.user) == key(user)
                && key(&ur.role) == key(role))
        });
        Ok(())
    }

    async fn user_roles_in(&self, account: &str, user: &str) -> Result<Vec<String>> {
        let state = self.state.read().await;
        Ok(state
            .user_roles
            .iter()
            .filter(|ur| ur.account.eq_ignore_ascii_case(account) && key(&ur.user) == key(user))
            .map(|ur| ur.role.clone())
            .collect())
    }

    async fn grant_role_to_role_in(&self, account: &str, child: &str, parent: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.roles.contains_key(&akey(account, child)) {
            return account_error::RoleNotFoundSnafu { role: child.to_string() }.fail();
        }
        if !state.roles.contains_key(&akey(account, parent)) {
            return account_error::RoleNotFoundSnafu { role: parent.to_string() }.fail();
        }
        if key(child) == key(parent) {
            return account_error::NotAllowedSnafu {
                message: format!("cannot grant role '{child}' to itself"),
            }
            .fail();
        }
        // A cycle forms when `parent` is already (transitively) beneath
        // `child` in the hierarchy: walk the children graph from `child`
        // and look for `parent` (Snowflake rejects circular hierarchies).
        let links: Vec<RoleRole> = state
            .role_roles
            .iter()
            .filter(|rr| rr.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect();
        if role_graph_reachable(&links, &key(child), &key(parent)) {
            return account_error::NotAllowedSnafu {
                message: format!(
                    "circular role hierarchy detected: role '{parent}' \
                     already inherits from '{child}'"
                ),
            }
            .fail();
        }
        let link = RoleRole {
            account: account.to_string(),
            child: child.to_string(),
            parent: parent.to_string(),
        };
        let exists = state.role_roles.iter().any(|rr| {
            rr.account.eq_ignore_ascii_case(account)
                && key(&rr.child) == key(child)
                && key(&rr.parent) == key(parent)
        });
        if !exists {
            state.role_roles.push(link.clone());
            self.journal_put(&crate::journal::AccountRecord::RoleRole(link))
                .await?;
        }
        Ok(())
    }

    async fn revoke_role_from_role_in(&self, account: &str, child: &str, parent: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<RoleRole> = state
            .role_roles
            .iter()
            .filter(|rr| {
                rr.account.eq_ignore_ascii_case(account)
                    && key(&rr.child) == key(child)
                    && key(&rr.parent) == key(parent)
            })
            .cloned()
            .collect();
        if removed.is_empty() {
            return account_error::GrantNotFoundSnafu {
                message: format!("role '{child}' is not granted to role '{parent}'"),
            }
            .fail();
        }
        for rr in &removed {
            self.journal_delete(&crate::journal::AccountRecord::RoleRole(rr.clone()))
                .await?;
        }
        state.role_roles.retain(|rr| {
            !(rr.account.eq_ignore_ascii_case(account)
                && key(&rr.child) == key(child)
                && key(&rr.parent) == key(parent))
        });
        Ok(())
    }

    async fn role_children_in(&self, account: &str, parent: &str) -> Result<Vec<String>> {
        let state = self.state.read().await;
        Ok(state
            .role_roles
            .iter()
            .filter(|rr| rr.account.eq_ignore_ascii_case(account) && key(&rr.parent) == key(parent))
            .map(|rr| rr.child.clone())
            .collect())
    }

    async fn effective_roles_in(&self, account: &str, roles: &[String]) -> Result<Vec<String>> {
        let state = self.state.read().await;
        let links: Vec<RoleRole> = state
            .role_roles
            .iter()
            .filter(|rr| rr.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect();
        // Resolve a lowercase key back to a display name: prefer the seed
        // (caller) casing, then the stored grant casing, then the key.
        let name_for = |k: &str| -> String {
            if let Some(r) = roles.iter().find(|r| key(r) == k) {
                return r.clone();
            }
            links
                .iter()
                .find(|rr| key(&rr.child) == k)
                .map(|rr| rr.child.clone())
                .unwrap_or_else(|| k.to_string())
        };
        let mut out: Vec<String> = Vec::new();
        let mut queue: Vec<String> = roles.iter().map(|r| key(r)).collect();
        while let Some(next) = queue.pop() {
            if out.iter().any(|r| key(r) == next) {
                continue;
            }
            out.push(name_for(&next));
            for rr in links.iter().filter(|rr| key(&rr.parent) == next) {
                let ck = key(&rr.child);
                if !queue.contains(&ck) && !out.iter().any(|r| key(r) == ck) {
                    queue.push(ck);
                }
            }
        }
        Ok(out)
    }

    async fn grant_in(&self, account: &str, grant: Grant) -> Result<()> {
        bump_account_epoch();
        let mut grant = grant;
        grant.account = account.to_string();
        let mut state = self.state.write().await;
        if !state.roles.contains_key(&akey(account, &grant.role)) {
            return account_error::RoleNotFoundSnafu {
                role: grant.role.clone(),
            }
            .fail();
        }
        let exists = state.grants.iter().any(|g| {
            g.account.eq_ignore_ascii_case(account)
                && key(&g.role) == key(&grant.role)
                && g.privilege == grant.privilege
                && g.object == grant.object
        });
        if !exists {
            state.grants.push(grant.clone());
            self.journal_put(&crate::journal::AccountRecord::Grant(grant.clone()))
                .await?;
        }
        Ok(())
    }

    async fn revoke_in(&self, account: &str, grant: &Grant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<Grant> = state
            .grants
            .iter()
            .filter(|g| {
                g.account.eq_ignore_ascii_case(account)
                    && key(&g.role) == key(&grant.role)
                    && g.privilege == grant.privilege
                    && g.object == grant.object
            })
            .cloned()
            .collect();
        if removed.is_empty() {
            return account_error::GrantNotFoundSnafu {
                message: format!(
                    "privilege '{}' on {} '{}' is not granted to role '{}'",
                    grant.privilege, grant.object.kind(), grant.object, grant.role
                ),
            }
            .fail();
        }
        for g in &removed {
            self.journal_delete(&crate::journal::AccountRecord::Grant(g.clone()))
                .await?;
        }
        state.grants.retain(|g| {
            !(g.account.eq_ignore_ascii_case(account)
                && key(&g.role) == key(&grant.role)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        Ok(())
    }

    async fn role_grants_in(&self, account: &str, role: &str) -> Result<Vec<Grant>> {
        let state = self.state.read().await;
        Ok(state
            .grants
            .iter()
            .filter(|g| g.account.eq_ignore_ascii_case(account) && key(&g.role) == key(role))
            .cloned()
            .collect())
    }

    async fn object_grants_in(&self, account: &str, object: &AccountObject) -> Result<Vec<Grant>> {
        let state = self.state.read().await;
        Ok(state
            .grants
            .iter()
            .filter(|g| g.account.eq_ignore_ascii_case(account))
            .filter(|g| grant_covers_object(&g.object, object))
            .cloned()
            .collect())
    }

    async fn share_grants_in(&self, account: &str, share: &str) -> Result<Vec<ShareGrant>> {
        let state = self.state.read().await;
        if !state.shares.contains_key(&akey(account, share)) {
            return account_error::ShareNotFoundSnafu { share: share.to_string() }.fail();
        }
        Ok(state
            .share_grants
            .iter()
            .filter(|g| g.account.eq_ignore_ascii_case(account) && key(&g.share) == key(share))
            .cloned()
            .collect())
    }

    async fn authenticate_in(&self, account: &str, login_name: &str, password: &str) -> Result<Option<User>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .filter(|u| u.account.eq_ignore_ascii_case(account))
            .find(|u| key(&u.login_name) == key(login_name))
            .filter(|u| !u.disabled && u.password == password)
            .cloned())
    }

    async fn record_login_in(&self, account: &str, event: LoginEvent) -> Result<()> {
        let mut event = event;
        event.account = account.to_string();
        event.sequence = self.login_seq.fetch_add(1, Ordering::Relaxed);
        self.journal_put(&crate::journal::AccountRecord::Login(event.clone()))
            .await?;
        self.state.write().await.logins.push(event);
        Ok(())
    }

    async fn list_logins_in(&self, account: &str) -> Result<Vec<LoginEvent>> {
        let state = self.state.read().await;
        Ok(state
            .logins
            .iter()
            .filter(|e| e.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect())
    }

    async fn allowed_in(
        &self,
        account: &str,
        role: &str,
        privilege: Privilege,
        object: &AccountObject,
    ) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.grants.iter().any(|g| {
            g.account.eq_ignore_ascii_case(account)
                && key(&g.role) == key(role)
                && g.privilege == privilege
                && g.object == *object
        }))
    }

    async fn create_share_in(&self, account: &str, name: &str) -> Result<Share> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.contains_key(&akey(account, name)) {
            return account_error::ShareAlreadyExistsSnafu { share: name.to_string() }.fail();
        }
        let share = Share {
            account: account.to_string(),
            name: name.to_string(),
            kind: crate::models::ShareKind::Outbound,
            provider: None,
        };
        state.shares.insert(akey(account, name), share.clone());
        self.journal_put(&crate::journal::AccountRecord::Share(share.clone()))
            .await?;
        Ok(share)
    }

    async fn create_inbound_share_in(&self, account: &str, name: &str, provider: &str) -> Result<Share> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.contains_key(&akey(account, name)) {
            return account_error::ShareAlreadyExistsSnafu { share: name.to_string() }.fail();
        }
        if !state.shares.contains_key(&akey(account, provider)) {
            return account_error::ShareNotFoundSnafu {
                share: provider.to_string(),
            }
            .fail();
        }
        let share = Share {
            account: account.to_string(),
            name: name.to_string(),
            kind: crate::models::ShareKind::Inbound,
            provider: Some(provider.to_string()),
        };
        state.shares.insert(akey(account, name), share.clone());
        self.journal_put(&crate::journal::AccountRecord::Share(share.clone()))
            .await?;
        Ok(share)
    }

    async fn drop_share_in(&self, account: &str, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.remove(&akey(account, name)).is_none() {
            return account_error::ShareNotFoundSnafu { share: name.to_string() }.fail();
        }
        let removed: Vec<ShareGrant> = state
            .share_grants
            .iter()
            .filter(|g| g.account.eq_ignore_ascii_case(account))
            .filter(|g| key(&g.share).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        for g in &removed {
            self.journal_delete(&crate::journal::AccountRecord::ShareGrant(g.clone()))
                .await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::Share(Share {
            account: account.to_string(),
            name: name.to_string(),
            kind: crate::models::ShareKind::Outbound,
            provider: None,
        }))
        .await?;
        state.share_grants.retain(|g| {
            !(g.account.eq_ignore_ascii_case(account)
                && key(&g.share).eq_ignore_ascii_case(&key(name)))
        });
        Ok(())
    }

    async fn get_share_in(&self, account: &str, name: &str) -> Result<Option<Share>> {
        let state = self.state.read().await;
        Ok(state.shares.get(&akey(account, name)).cloned())
    }

    async fn list_shares_in(&self, account: &str) -> Result<Vec<Share>> {
        let state = self.state.read().await;
        Ok(state
            .shares
            .values()
            .filter(|s| s.account.eq_ignore_ascii_case(account))
            .cloned()
            .collect())
    }

    async fn grant_to_share_in(&self, account: &str, grant: ShareGrant) -> Result<()> {
        bump_account_epoch();
        let mut grant = grant;
        grant.account = account.to_string();
        let mut state = self.state.write().await;
        if !state.shares.contains_key(&akey(account, &grant.share)) {
            return account_error::ShareNotFoundSnafu {
                share: grant.share.clone(),
            }
            .fail();
        }
        let exists = state.share_grants.iter().any(|g| {
            g.account.eq_ignore_ascii_case(account)
                && key(&g.share) == key(&grant.share)
                && g.privilege == grant.privilege
                && g.object == grant.object
        });
        if !exists {
            state.share_grants.push(grant.clone());
            self.journal_put(&crate::journal::AccountRecord::ShareGrant(grant.clone()))
                .await?;
        }
        Ok(())
    }

    async fn revoke_from_share_in(&self, account: &str, grant: &ShareGrant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<ShareGrant> = state
            .share_grants
            .iter()
            .filter(|g| {
                g.account.eq_ignore_ascii_case(account)
                    && key(&g.share) == key(&grant.share)
                    && g.privilege == grant.privilege
                    && g.object == grant.object
            })
            .cloned()
            .collect();
        if removed.is_empty() {
            return account_error::GrantNotFoundSnafu {
                message: format!(
                    "privilege '{}' on {} '{}' is not granted to share '{}'",
                    grant.privilege, grant.object.kind(), grant.object, grant.share
                ),
            }
            .fail();
        }
        for g in &removed {
            self.journal_delete(&crate::journal::AccountRecord::ShareGrant(g.clone()))
                .await?;
        }
        state.share_grants.retain(|g| {
            !(g.account.eq_ignore_ascii_case(account)
                && key(&g.share) == key(&grant.share)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        Ok(())
    }

    // ================= accounts (org-level) =================

    async fn create_account_in(
        &self,
        name: &str,
        admin_name: &str,
        admin_password: &str,
    ) -> Result<Account> {
        bump_account_epoch();
        {
            let state = self.state.read().await;
            if name.eq_ignore_ascii_case(DEFAULT_ACCOUNT) {
                return account_error::AccountAlreadyExistsSnafu {
                    account: name.to_string(),
                }
                .fail();
            }
            if state.accounts.contains_key(&key(name)) {
                return account_error::AccountAlreadyExistsSnafu {
                    account: name.to_string(),
                }
                .fail();
            }
            if admin_name.is_empty() || admin_password.is_empty() {
                return account_error::NotAllowedSnafu {
                    message: "CREATE ACCOUNT requires ADMIN_NAME and ADMIN_PASSWORD".to_string(),
                }
                .fail();
            }
        }
        let account = Account {
            name: name.to_string(),
        };
        {
            let mut state = self.state.write().await;
            state.accounts.insert(key(name), account.clone());
        }
        self.journal_put(&crate::journal::AccountRecord::Account(account.clone()))
            .await?;
        // Seed the account's ACCOUNTADMIN role + initial admin user
        // (Snowflake requires ADMIN_NAME/ADMIN_PASSWORD at CREATE ACCOUNT).
        self.create_role_in(name, ACCOUNTADMIN).await?;
        self.create_user_in(
            name,
            User {
                account: name.to_string(),
                name: admin_name.to_string(),
                login_name: admin_name.to_string(),
                password: admin_password.to_string(),
                default_role: Some(ACCOUNTADMIN.to_string()),
                tenant: None,
                disabled: false,
            },
        )
        .await?;
        self.grant_role_to_user_in(name, admin_name, ACCOUNTADMIN)
            .await?;
        Ok(account)
    }

    async fn drop_account_in(&self, name: &str) -> Result<()> {
        bump_account_epoch();
        if name.eq_ignore_ascii_case(DEFAULT_ACCOUNT) {
            return account_error::NotAllowedSnafu {
                message: format!(
                    "cannot drop the built-in account '{DEFAULT_ACCOUNT}'"
                ),
            }
            .fail();
        }
        let mut state = self.state.write().await;
        if state.accounts.remove(&key(name)).is_none() {
            return account_error::AccountNotFoundSnafu {
                account: name.to_string(),
            }
            .fail();
        }
        drop(state);
        // Cascade: journalize and remove every record the account owns.
        let users = self.list_users_in(name).await?;
        let roles = self.list_roles_in(name).await?;
        let shares = self.list_shares_in(name).await?;
        for u in users {
            self.drop_user_in(name, &u.name).await?;
        }
        for r in roles {
            self.drop_role_in(name, &r.name).await?;
        }
        for s in shares {
            self.drop_share_in(name, &s.name).await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::Account(Account {
            name: name.to_string(),
        }))
        .await?;
        Ok(())
    }

    async fn list_accounts_in(&self) -> Result<Vec<Account>> {
        let state = self.state.read().await;
        let mut out: Vec<Account> = state.accounts.values().cloned().collect();
        // The default account is implicit; surface it for SHOW ACCOUNTS.
        if !out.iter().any(|a| a.name.eq_ignore_ascii_case(DEFAULT_ACCOUNT)) {
            out.push(Account {
                name: DEFAULT_ACCOUNT.to_string(),
            });
        }
        out.sort_by(|a, b| a.name.to_ascii_lowercase().cmp(&b.name.to_ascii_lowercase()));
        Ok(out)
    }

    async fn get_account_in(&self, name: &str) -> Result<Option<Account>> {
        let state = self.state.read().await;
        if name.eq_ignore_ascii_case(DEFAULT_ACCOUNT) {
            return Ok(Some(Account {
                name: DEFAULT_ACCOUNT.to_string(),
            }));
        }
        Ok(state.accounts.get(&key(name)).cloned())
    }
}

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// In-memory + journal key for an account-owned record: the default
/// account keeps the bare (pre-M15.4) key; other accounts qualify it.
fn akey(account: &str, name: &str) -> String {
    crate::journal::account_key(account, key(name))
}

/// Non-blocking read of the shared state (fast path for `scope`).
fn try_lock_read_noio(
    state: &Arc<RwLock<StoreState>>,
) -> Option<tokio::sync::RwLockReadGuard<'_, StoreState>> {
    state.try_read().ok()
}

#[async_trait]
impl AccountStore for InMemoryAccountStore {
    async fn create_role(&self, name: &str) -> Result<Role> {
        self.create_role_in(DEFAULT_ACCOUNT, name).await
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        self.drop_role_in(DEFAULT_ACCOUNT, name).await
    }

    async fn get_role(&self, name: &str) -> Result<Option<Role>> {
        self.get_role_in(DEFAULT_ACCOUNT, name).await
    }

    async fn list_roles(&self) -> Result<Vec<Role>> {
        self.list_roles_in(DEFAULT_ACCOUNT).await
    }

    async fn create_user(&self, user: User) -> Result<User> {
        self.create_user_in(DEFAULT_ACCOUNT, user).await
    }

    async fn drop_user(&self, name: &str) -> Result<()> {
        self.drop_user_in(DEFAULT_ACCOUNT, name).await
    }

    async fn get_user(&self, login_name: &str) -> Result<Option<User>> {
        self.get_user_in(DEFAULT_ACCOUNT, login_name).await
    }

    async fn list_users(&self) -> Result<Vec<User>> {
        self.list_users_in(DEFAULT_ACCOUNT).await
    }

    async fn grant_role_to_user(&self, user: &str, role: &str) -> Result<()> {
        self.grant_role_to_user_in(DEFAULT_ACCOUNT, user, role).await
    }

    async fn revoke_role_from_user(&self, user: &str, role: &str) -> Result<()> {
        self.revoke_role_from_user_in(DEFAULT_ACCOUNT, user, role).await
    }

    async fn user_roles(&self, user: &str) -> Result<Vec<String>> {
        self.user_roles_in(DEFAULT_ACCOUNT, user).await
    }

    async fn grant_role_to_role(&self, child: &str, parent: &str) -> Result<()> {
        self.grant_role_to_role_in(DEFAULT_ACCOUNT, child, parent).await
    }

    async fn revoke_role_from_role(&self, child: &str, parent: &str) -> Result<()> {
        self.revoke_role_from_role_in(DEFAULT_ACCOUNT, child, parent).await
    }

    async fn role_children(&self, parent: &str) -> Result<Vec<String>> {
        self.role_children_in(DEFAULT_ACCOUNT, parent).await
    }

    async fn effective_roles(&self, roles: &[String]) -> Result<Vec<String>> {
        self.effective_roles_in(DEFAULT_ACCOUNT, roles).await
    }

    async fn grant(&self, grant: Grant) -> Result<()> {
        self.grant_in(DEFAULT_ACCOUNT, grant).await
    }

    async fn revoke(&self, grant: &Grant) -> Result<()> {
        self.revoke_in(DEFAULT_ACCOUNT, grant).await
    }

    async fn role_grants(&self, role: &str) -> Result<Vec<Grant>> {
        self.role_grants_in(DEFAULT_ACCOUNT, role).await
    }

    async fn object_grants(&self, object: &AccountObject) -> Result<Vec<Grant>> {
        self.object_grants_in(DEFAULT_ACCOUNT, object).await
    }

    async fn create_share(&self, name: &str) -> Result<Share> {
        self.create_share_in(DEFAULT_ACCOUNT, name).await
    }

    async fn create_inbound_share(&self, name: &str, provider: &str) -> Result<Share> {
        self.create_inbound_share_in(DEFAULT_ACCOUNT, name, provider).await
    }

    async fn drop_share(&self, name: &str) -> Result<()> {
        self.drop_share_in(DEFAULT_ACCOUNT, name).await
    }

    async fn get_share(&self, name: &str) -> Result<Option<Share>> {
        self.get_share_in(DEFAULT_ACCOUNT, name).await
    }

    async fn list_shares(&self) -> Result<Vec<Share>> {
        self.list_shares_in(DEFAULT_ACCOUNT).await
    }

    async fn grant_to_share(&self, grant: ShareGrant) -> Result<()> {
        self.grant_to_share_in(DEFAULT_ACCOUNT, grant).await
    }

    async fn revoke_from_share(&self, grant: &ShareGrant) -> Result<()> {
        self.revoke_from_share_in(DEFAULT_ACCOUNT, grant).await
    }

    async fn share_grants(&self, share: &str) -> Result<Vec<ShareGrant>> {
        self.share_grants_in(DEFAULT_ACCOUNT, share).await
    }

    async fn create_account(
        &self,
        name: &str,
        admin_name: &str,
        admin_password: &str,
    ) -> Result<Account> {
        self.create_account_in(name, admin_name, admin_password).await
    }

    async fn drop_account(&self, name: &str) -> Result<()> {
        self.drop_account_in(name).await
    }

    async fn list_accounts(&self) -> Result<Vec<Account>> {
        self.list_accounts_in().await
    }

    async fn get_account(&self, name: &str) -> Result<Option<Account>> {
        self.get_account_in(name).await
    }

    async fn account_scope(self: Arc<Self>, account: &str) -> Result<Option<Arc<dyn AccountStore>>> {
        Ok(self
            .scope(&account)
            .map(|s| Arc::new(s) as Arc<dyn AccountStore>))
    }

    async fn authenticate(&self, login_name: &str, password: &str) -> Result<Option<User>> {
        self.authenticate_in(DEFAULT_ACCOUNT, login_name, password).await
    }

    async fn record_login(&self, event: LoginEvent) -> Result<()> {
        self.record_login_in(DEFAULT_ACCOUNT, event).await
    }

    async fn list_logins(&self) -> Result<Vec<LoginEvent>> {
        self.list_logins_in(DEFAULT_ACCOUNT).await
    }

    async fn allowed(
        &self,
        role: &str,
        privilege: Privilege,
        object: &AccountObject,
    ) -> Result<bool> {
        self.allowed_in(DEFAULT_ACCOUNT, role, privilege, object).await
    }
}

#[async_trait]
impl AccountStore for ScopedAccountStore {
    async fn create_role(&self, name: &str) -> Result<Role> {
        self.inner.create_role_in(&self.account, name).await
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        self.inner.drop_role_in(&self.account, name).await
    }

    async fn get_role(&self, name: &str) -> Result<Option<Role>> {
        self.inner.get_role_in(&self.account, name).await
    }

    async fn list_roles(&self) -> Result<Vec<Role>> {
        self.inner.list_roles_in(&self.account).await
    }

    async fn create_user(&self, user: User) -> Result<User> {
        self.inner.create_user_in(&self.account, user).await
    }

    async fn drop_user(&self, name: &str) -> Result<()> {
        self.inner.drop_user_in(&self.account, name).await
    }

    async fn get_user(&self, login_name: &str) -> Result<Option<User>> {
        self.inner.get_user_in(&self.account, login_name).await
    }

    async fn list_users(&self) -> Result<Vec<User>> {
        self.inner.list_users_in(&self.account).await
    }

    async fn grant_role_to_user(&self, user: &str, role: &str) -> Result<()> {
        self.inner.grant_role_to_user_in(&self.account, user, role).await
    }

    async fn revoke_role_from_user(&self, user: &str, role: &str) -> Result<()> {
        self.inner.revoke_role_from_user_in(&self.account, user, role).await
    }

    async fn user_roles(&self, user: &str) -> Result<Vec<String>> {
        self.inner.user_roles_in(&self.account, user).await
    }

    async fn grant_role_to_role(&self, child: &str, parent: &str) -> Result<()> {
        self.inner.grant_role_to_role_in(&self.account, child, parent).await
    }

    async fn revoke_role_from_role(&self, child: &str, parent: &str) -> Result<()> {
        self.inner.revoke_role_from_role_in(&self.account, child, parent).await
    }

    async fn role_children(&self, parent: &str) -> Result<Vec<String>> {
        self.inner.role_children_in(&self.account, parent).await
    }

    async fn effective_roles(&self, roles: &[String]) -> Result<Vec<String>> {
        self.inner.effective_roles_in(&self.account, roles).await
    }

    async fn grant(&self, grant: Grant) -> Result<()> {
        self.inner.grant_in(&self.account, grant).await
    }

    async fn revoke(&self, grant: &Grant) -> Result<()> {
        self.inner.revoke_in(&self.account, grant).await
    }

    async fn role_grants(&self, role: &str) -> Result<Vec<Grant>> {
        self.inner.role_grants_in(&self.account, role).await
    }

    async fn object_grants(&self, object: &AccountObject) -> Result<Vec<Grant>> {
        self.inner.object_grants_in(&self.account, object).await
    }

    async fn create_share(&self, name: &str) -> Result<Share> {
        self.inner.create_share_in(&self.account, name).await
    }

    async fn create_inbound_share(&self, name: &str, provider: &str) -> Result<Share> {
        self.inner.create_inbound_share_in(&self.account, name, provider).await
    }

    async fn drop_share(&self, name: &str) -> Result<()> {
        self.inner.drop_share_in(&self.account, name).await
    }

    async fn get_share(&self, name: &str) -> Result<Option<Share>> {
        self.inner.get_share_in(&self.account, name).await
    }

    async fn list_shares(&self) -> Result<Vec<Share>> {
        self.inner.list_shares_in(&self.account).await
    }

    async fn grant_to_share(&self, grant: ShareGrant) -> Result<()> {
        self.inner.grant_to_share_in(&self.account, grant).await
    }

    async fn revoke_from_share(&self, grant: &ShareGrant) -> Result<()> {
        self.inner.revoke_from_share_in(&self.account, grant).await
    }

    async fn share_grants(&self, share: &str) -> Result<Vec<ShareGrant>> {
        self.inner.share_grants_in(&self.account, share).await
    }

    async fn create_account(
        &self,
        name: &str,
        admin_name: &str,
        admin_password: &str,
    ) -> Result<Account> {
        self.inner.create_account(name, admin_name, admin_password).await
    }

    async fn drop_account(&self, name: &str) -> Result<()> {
        self.inner.drop_account(name).await
    }

    async fn list_accounts(&self) -> Result<Vec<Account>> {
        self.inner.list_accounts().await
    }

    async fn get_account(&self, name: &str) -> Result<Option<Account>> {
        self.inner.get_account(name).await
    }

    async fn account_scope(self: Arc<Self>, account: &str) -> Result<Option<Arc<dyn AccountStore>>> {
        self.inner.clone().account_scope(account).await
    }

    async fn authenticate(&self, login_name: &str, password: &str) -> Result<Option<User>> {
        self.inner.authenticate_in(&self.account, login_name, password).await
    }

    async fn record_login(&self, event: LoginEvent) -> Result<()> {
        self.inner.record_login_in(&self.account, event).await
    }

    async fn list_logins(&self) -> Result<Vec<LoginEvent>> {
        self.inner.list_logins_in(&self.account).await
    }

    async fn allowed(
        &self,
        role: &str,
        privilege: Privilege,
        object: &AccountObject,
    ) -> Result<bool> {
        self.inner.allowed_in(&self.account, role, privilege, object).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> InMemoryAccountStore {
        InMemoryAccountStore::new().with_admin("embucket", "embucket")
    }

    #[tokio::test]
    async fn admin_bootstrap_and_auth() {
        let s = store();
        let user = s.authenticate("embucket", "embucket").await.unwrap();
        assert!(user.is_some());
        assert!(s.authenticate("embucket", "wrong").await.unwrap().is_none());
        assert!(s.authenticate("nobody", "x").await.unwrap().is_none());
        let roles = s.user_roles("embucket").await.unwrap();
        assert_eq!(roles, vec![ACCOUNTADMIN.to_string()]);
    }

    #[tokio::test]
    async fn roles_and_grants_lifecycle() {
        let s = store();
        s.create_role("analyst").await.unwrap();
        assert!(s.create_role("analyst").await.is_err());
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        assert!(s
            .allowed(
                "analyst",
                Privilege::Select,
                &AccountObject::Table("db".into(), "public".into(), "t".into())
            )
            .await
            .unwrap());
        assert!(!s
            .allowed(
                "analyst",
                Privilege::Usage,
                &AccountObject::Database("db".into())
            )
            .await
            .unwrap());
        // Revoke twice → second fails with GrantNotFound.
        s.revoke(&Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        assert!(s
            .revoke(&Grant {
                                account: DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Select,
                object: AccountObject::Table("db".into(), "public".into(), "t".into()),
            })
            .await
            .is_err());
        // Drop cascades grants and user links.
        s.grant_role_to_user("embucket", "analyst").await.unwrap();
        s.drop_role("analyst").await.unwrap();
        // The bootstrap ACCOUNTADMIN link survives; only the analyst link goes.
        let roles = s.user_roles("embucket").await.unwrap();
        assert!(!roles.iter().any(|r| r.eq_ignore_ascii_case("analyst")));
    }

    #[tokio::test]
    async fn shares_lifecycle() {
        let s = store();
        assert!(s.get_share("vendor").await.unwrap().is_none());
        s.create_share("vendor").await.unwrap();
        assert!(s.create_share("vendor").await.is_err());
        assert!(s.get_share("VENDOR").await.unwrap().is_some());

        let grant = ShareGrant {
                        account: DEFAULT_ACCOUNT.into(),
share: "vendor".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        };
        s.grant_to_share(grant.clone()).await.unwrap();
        // Duplicate grant is idempotent.
        s.grant_to_share(grant.clone()).await.unwrap();
        assert_eq!(s.share_grants("vendor").await.unwrap(), vec![grant.clone()]);
        // Grants to unknown shares fail.
        assert!(s
            .grant_to_share(ShareGrant {
                                account: DEFAULT_ACCOUNT.into(),
share: "nope".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Database("db".into()),
            })
            .await
            .is_err());
        assert!(s.share_grants("nope").await.is_err());
        // Revoke works; second revoke reports not-found.
        s.revoke_from_share(&grant).await.unwrap();
        assert!(s.revoke_from_share(&grant).await.is_err());
        assert_eq!(s.list_shares().await.unwrap().len(), 1);

        // Drop cascades the share's grants.
        s.grant_to_share(grant).await.unwrap();
        s.drop_share("vendor").await.unwrap();
        assert!(s.drop_share("vendor").await.is_err());
        assert!(s.share_grants("vendor").await.is_err());
        assert!(s.list_shares().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn inbound_shares_are_a_separate_kind() {
        let s = store();
        s.create_share("vendor").await.unwrap();
        // Same name in either kind collides.
        assert!(s.create_inbound_share("vendor", "vendor").await.is_err());
        // Provider must exist.
        assert!(s.create_inbound_share("acme_in", "nope").await.is_err());
        let inbound = s.create_inbound_share("acme_in", "vendor").await.unwrap();
        assert_eq!(inbound.kind, crate::models::ShareKind::Inbound);
        assert_eq!(inbound.provider.as_deref(), Some("vendor"));
        let kinds: Vec<_> = s
            .list_shares()
            .await
            .unwrap()
            .into_iter()
            .map(|sh| (sh.name, sh.kind))
            .collect();
        assert!(kinds.contains(&("vendor".to_string(), crate::models::ShareKind::Outbound)));
        assert!(kinds.contains(&("acme_in".to_string(), crate::models::ShareKind::Inbound)));
    }

    #[tokio::test]
    async fn user_create_and_link() {
        let s = store();
        s.create_role("r1").await.unwrap();
        s.create_user(User {
                        account: DEFAULT_ACCOUNT.into(),
name: "u1".into(),
            login_name: "u1".into(),
            password: "pw".into(),
            default_role: Some("r1".into()),
            tenant: Some("acme".into()),
            disabled: false,
        })
        .await
        .unwrap();
        assert!(s.create_user(User {
                        account: DEFAULT_ACCOUNT.into(),
name: "u1".into(),
            login_name: "x".into(),
            password: String::new(),
            default_role: None,
            tenant: None,
            disabled: false,
        })
        .await
        .is_err());
        s.grant_role_to_user("u1", "r1").await.unwrap();
        assert_eq!(s.user_roles("u1").await.unwrap(), vec!["r1"]);
        // Lookup by login name works.
        assert!(s.get_user("u1").await.unwrap().is_some());
        // Unknown user/role link errors.
        assert!(s.grant_role_to_user("nobody", "r1").await.is_err());
        assert!(s.grant_role_to_user("u1", "nope").await.is_err());
    }

    // ================= journal (M17.3.1) =================

    /// In-memory [`AccountJournal`] backend for tests — mimics the raw-KV
    /// state store (domain → key → value).
    #[derive(Default)]
    struct MockJournal(std::sync::Mutex<HashMap<String, HashMap<String, Vec<u8>>>>);

    impl std::fmt::Debug for MockJournal {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("MockJournal")
        }
    }

    #[async_trait::async_trait]
    impl crate::journal::AccountJournal for MockJournal {
        async fn put(&self, domain: &str, k: &str, value: Vec<u8>) -> crate::journal::JournalResult<()> {
            self.0
                .lock()
                .unwrap()
                .entry(domain.to_string())
                .or_default()
                .insert(k.to_string(), value);
            Ok(())
        }

        async fn delete(&self, domain: &str, k: &str) -> crate::journal::JournalResult<()> {
            if let Some(m) = self.0.lock().unwrap().get_mut(domain) {
                m.remove(k);
            }
            Ok(())
        }

        async fn list(&self, domain: &str) -> crate::journal::JournalResult<Vec<(String, Vec<u8>)>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(domain)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default())
        }
    }

    #[tokio::test]
    async fn journal_replay_restores_full_state() {
        let journal = Arc::new(MockJournal::default());
        let s = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        s.ensure_admin("embucket", "embucket").await.unwrap();
        s.create_role("analyst").await.unwrap();
        s.create_user(User {
                        account: DEFAULT_ACCOUNT.into(),
name: "u1".into(),
            login_name: "u1".into(),
            password: "pw".into(),
            default_role: Some("analyst".into()),
            tenant: Some("acme".into()),
            disabled: false,
        })
        .await
        .unwrap();
        s.grant_role_to_user("u1", "analyst").await.unwrap();
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        s.create_share("vendor").await.unwrap();
        s.grant_to_share(ShareGrant {
                        account: DEFAULT_ACCOUNT.into(),
share: "vendor".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("db".into()),
        })
        .await
        .unwrap();
        s.record_login(LoginEvent {
                        account: DEFAULT_ACCOUNT.into(),
timestamp_ms: 1_000,
            sequence: 0,
            user_name: "u1".into(),
            client_app_id: None,
            client_ip: None,
            success: true,
        })
        .await
        .unwrap();
        s.record_login(LoginEvent {
                        account: DEFAULT_ACCOUNT.into(),
timestamp_ms: 2_000,
            sequence: 0,
            user_name: "u1".into(),
            client_app_id: None,
            client_ip: None,
            success: false,
        })
        .await
        .unwrap();

        // "Restart": a fresh store over the same journal.
        let s2 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        assert!(s2.authenticate("embucket", "embucket").await.unwrap().is_some());
        assert!(s2.authenticate("u1", "pw").await.unwrap().is_some());
        assert_eq!(s2.user_roles("u1").await.unwrap(), vec!["analyst"]);
        assert!(s2
            .allowed(
                "analyst",
                Privilege::Select,
                &AccountObject::Table("db".into(), "public".into(), "t".into())
            )
            .await
            .unwrap());
        assert_eq!(s2.share_grants("vendor").await.unwrap().len(), 1);
        // Logins replay oldest-first.
        let logins = s2.list_logins().await.unwrap();
        assert_eq!(logins.len(), 2);
        assert!(logins[0].timestamp_ms <= logins[1].timestamp_ms);
        assert!(logins[0].success); // ts=1000 success
        assert!(!logins[1].success); // ts=2000 failure
    }

    #[tokio::test]
    async fn journal_replay_applies_deletes_and_cascades() {
        let journal = Arc::new(MockJournal::default());
        let s = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        s.ensure_admin("embucket", "embucket").await.unwrap();
        s.create_role("analyst").await.unwrap();
        s.grant_role_to_user("embucket", "analyst").await.unwrap();
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("db".into()),
        })
        .await
        .unwrap();
        // Drop cascades; the journal must reflect the removals.
        s.drop_role("analyst").await.unwrap();

        let s2 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        assert!(s2.get_role("analyst").await.unwrap().is_none());
        // The analyst link is gone; the bootstrap ACCOUNTADMIN link survives.
        assert_eq!(
            s2.user_roles("embucket").await.unwrap(),
            vec![ACCOUNTADMIN.to_string()]
        );
        assert!(s2.role_grants("analyst").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn ensure_admin_is_idempotent_across_restarts() {
        let journal = Arc::new(MockJournal::default());
        let s = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        s.ensure_admin("embucket", "embucket").await.unwrap();
        // Second seed on replayed state: no error, no duplicate.
        let s2 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        s2.ensure_admin("embucket", "embucket").await.unwrap();
        assert_eq!(s2.list_users().await.unwrap().len(), 1);
        assert_eq!(s2.list_roles().await.unwrap().len(), 1);
        assert_eq!(s2.user_roles("embucket").await.unwrap().len(), 1);
        // Renamed password does not overwrite: ensure_admin no-ops.
        s2.ensure_admin("embucket", "other").await.unwrap();
        assert!(s2.authenticate("embucket", "embucket").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn account_epoch_bumps_on_authz_mutations_only() {
        // The epoch counter is process-global (result-cache fingerprint,
        // §23.5) and sibling tests bump it concurrently — so every
        // assertion here brackets exactly ONE operation (delta across a
        // single await, not a chain).
        let s = InMemoryAccountStore::new();
        let before = super::account_epoch();
        s.create_role("e1").await.unwrap();
        assert!(super::account_epoch() > before, "create_role bumps");
        let event = LoginEvent {
                        account: DEFAULT_ACCOUNT.into(),
timestamp_ms: 0,
            sequence: 0,
            user_name: "e1".into(),
            client_app_id: None,
            client_ip: None,
            success: true,
        };
        let before_login = super::account_epoch();
        s.record_login(event).await.unwrap();
        assert_eq!(
            super::account_epoch(),
            before_login,
            "record_login must not bump (no authorization effect)"
        );
        let before_grant = super::account_epoch();
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "e1".to_string(),
            privilege: crate::models::Privilege::Select,
            object: crate::models::AccountObject::Table(
                "db".to_string(),
                "s".to_string(),
                "t".to_string(),
            ),
        })
        .await
        .unwrap();
        assert!(super::account_epoch() > before_grant, "grant bumps");
        let before_revoke = super::account_epoch();
        s.revoke(&Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "e1".to_string(),
            privilege: crate::models::Privilege::Select,
            object: crate::models::AccountObject::Table(
                "db".to_string(),
                "s".to_string(),
                "t".to_string(),
            ),
        })
        .await
        .unwrap();
        assert!(super::account_epoch() > before_revoke, "revoke bumps");
    }

    #[tokio::test]
    async fn role_hierarchy_grant_revoke_and_effective() {
        let s = store();
        for name in ["junior", "analyst", "senior"] {
            s.create_role(name).await.unwrap();
        }
        // Chain: junior -> analyst -> senior (senior inherits both).
        s.grant_role_to_role("junior", "analyst").await.unwrap();
        s.grant_role_to_role("analyst", "senior").await.unwrap();
        // Idempotent re-grant.
        s.grant_role_to_role("junior", "analyst").await.unwrap();

        assert_eq!(
            s.role_children("analyst").await.unwrap(),
            vec!["junior".to_string()]
        );
        let eff = s.effective_roles(&["senior".to_string()]).await.unwrap();
        for name in ["senior", "analyst", "junior"] {
            assert!(eff.iter().any(|r| r.eq_ignore_ascii_case(name)), "{:?}", eff);
        }
        assert_eq!(eff.len(), 3);
        // Seeds keep their casing; inherited roles come from the grants.
        let eff = s.effective_roles(&["Analyst".to_string()]).await.unwrap();
        assert_eq!(eff.len(), 2);

        // Revoke cuts the chain.
        s.revoke_role_from_role("analyst", "senior").await.unwrap();
        let eff = s.effective_roles(&["senior".to_string()]).await.unwrap();
        assert_eq!(eff.len(), 1);
        // Revoking a non-existent link errors.
        assert!(s.revoke_role_from_role("analyst", "senior").await.is_err());
    }

    #[tokio::test]
    async fn role_hierarchy_rejects_cycles_and_missing_roles() {
        let s = store();
        for name in ["a", "b", "c"] {
            s.create_role(name).await.unwrap();
        }
        // Self-grant.
        assert!(s.grant_role_to_role("a", "a").await.is_err());
        // Direct cycle: grant a to b, then b to a.
        s.grant_role_to_role("a", "b").await.unwrap();
        assert!(s.grant_role_to_role("b", "a").await.is_err());
        // Indirect cycle: a->b->c, then c to a.
        s.grant_role_to_role("b", "c").await.unwrap();
        let err = s.grant_role_to_role("c", "a").await.unwrap_err().to_string();
        assert!(err.contains("circular"), "{err}");
        // Missing roles.
        assert!(s.grant_role_to_role("ghost", "a").await.is_err());
        assert!(s.grant_role_to_role("a", "ghost").await.is_err());
    }

    #[tokio::test]
    async fn drop_role_cascades_role_role_links() {
        let journal = Arc::new(MockJournal::default());
        let s = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        s.ensure_admin("embucket", "embucket").await.unwrap();
        for name in ["junior", "mid", "senior"] {
            s.create_role(name).await.unwrap();
        }
        s.grant_role_to_role("junior", "mid").await.unwrap();
        s.grant_role_to_role("mid", "senior").await.unwrap();
        // Dropping `mid` removes both links (child and parent side).
        s.drop_role("mid").await.unwrap();
        assert!(s.role_children("senior").await.unwrap().is_empty());

        let s2 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        assert!(s2.role_children("senior").await.unwrap().is_empty());
        // junior still exists with no parents.
        assert_eq!(
            s2.effective_roles(&["junior".to_string()]).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn object_grants_spans_roles() {
        let s = store();
        for name in ["r1", "r2"] {
            s.create_role(name).await.unwrap();
        }
        let t = AccountObject::Table("db".into(), "s".into(), "t".into());
        for role in ["r1", "r2"] {
            s.grant(Grant {
                                account: DEFAULT_ACCOUNT.into(),
role: role.into(),
                privilege: Privilege::Select,
                object: t.clone(),
            })
            .await
            .unwrap();
        }
        // Ownership grant from a third role on the same object.
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "r1".into(),
            privilege: Privilege::Ownership,
            object: t.clone(),
        })
        .await
        .unwrap();
        let grants = s.object_grants(&t).await.unwrap();
        assert_eq!(grants.len(), 3, "{grants:?}");
        // Unrelated object grants are not matched.
        assert!(s
            .object_grants(&AccountObject::Table("db".into(), "s".into(), "other".into()))
            .await
            .unwrap()
            .is_empty());
        // Containment (design §23.7): a schema grant is covered by the
        // database and covers its tables.
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "r2".into(),
            privilege: Privilege::Ownership,
            object: AccountObject::Schema("db".into(), "s".into()),
        })
        .await
        .unwrap();
        s.grant(Grant {
                        account: DEFAULT_ACCOUNT.into(),
role: "r1".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("db".into()),
        })
        .await
        .unwrap();
        let sobj = AccountObject::Schema("db".into(), "s".into());
        let dobj = AccountObject::Database("db".into());
        assert_eq!(
            s.object_grants(&sobj).await.unwrap().len(),
            4,
            "schema grant + 3 table grants inside the schema"
        );
        assert_eq!(
            s.object_grants(&dobj).await.unwrap().len(),
            5,
            "db grant + schema grant + 3 table grants"
        );
        assert!(s
            .object_grants(&AccountObject::Database("otherdb".into()))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn journal_replays_role_roles() {
        let journal = Arc::new(MockJournal::default());
        let s = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        s.ensure_admin("embucket", "embucket").await.unwrap();
        for name in ["jr", "sr"] {
            s.create_role(name).await.unwrap();
        }
        s.grant_role_to_role("jr", "sr").await.unwrap();

        let s2 = InMemoryAccountStore::new_with_journal(journal.clone())
            .await
            .unwrap();
        let eff = s2.effective_roles(&["sr".to_string()]).await.unwrap();
        assert_eq!(eff.len(), 2, "{:?}", eff);
        // Idempotent replay: reload once more.
        let s3 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        assert_eq!(
            s3.effective_roles(&["sr".to_string()]).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn accounts_create_seed_admin_and_isolate() {
        let s = Arc::new(store());
        s.create_account("acme", "acmeadmin", "pw1").await.unwrap();
        assert!(s.create_account("acme", "x", "y").await.is_err());
        // The built-in account cannot be re-created or dropped.
        assert!(s.create_account(DEFAULT_ACCOUNT, "x", "y").await.is_err());
        assert!(s.drop_account(DEFAULT_ACCOUNT).await.is_err());
        // Seeded admin authenticates only inside the new account.
        assert!(s.authenticate("acmeadmin", "pw1").await.unwrap().is_none());
        let scope = Arc::clone(&s).account_scope("acme").await.unwrap().unwrap();
        assert!(scope.authenticate("acmeadmin", "pw1").await.unwrap().is_some());
        assert!(scope.authenticate("acmeadmin", "bad").await.unwrap().is_none());
        assert!(scope.authenticate("embucket", "embucket").await.unwrap().is_none());
        // The default account's admin is invisible from the new account.
        assert!(scope.get_user("embucket").await.unwrap().is_none());
        // Roles are per-account: each account sees only its own.
        let default_roles = s.list_roles().await.unwrap();
        let acme_roles = scope.list_roles().await.unwrap();
        assert!(default_roles.iter().all(|r| r.account == DEFAULT_ACCOUNT));
        assert!(acme_roles.iter().all(|r| r.account == "acme"));
        // Same role name in two accounts does not collide.
        scope.create_role("analyst").await.unwrap();
        s.create_role("analyst").await.unwrap();
        scope.grant(Grant {
            account: "acme".into(),
            role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("acmedb".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        assert!(scope
            .allowed("analyst", Privilege::Select, &AccountObject::Table("acmedb".into(), "public".into(), "t".into()))
            .await
            .unwrap());
        assert!(!s
            .allowed("analyst", Privilege::Select, &AccountObject::Table("acmedb".into(), "public".into(), "t".into()))
            .await
            .unwrap());
        // Unknown account → no scope.
        assert!(Arc::clone(&s).account_scope("ghost").await.unwrap().is_none());
        // SHOW ACCOUNTS surfaces both.
        let names: Vec<String> =
            s.list_accounts().await.unwrap().into_iter().map(|a| a.name).collect();
        assert!(names.iter().any(|n| n.eq_ignore_ascii_case("acme")));
        assert!(names.iter().any(|n| n.eq_ignore_ascii_case(DEFAULT_ACCOUNT)));
    }

    #[tokio::test]
    async fn drop_account_cascades_records() {
        let journal = Arc::new(MockJournal::default());
        let s = Arc::new(
            InMemoryAccountStore::new_with_journal(journal.clone())
                .await
                .unwrap(),
        );
        s.ensure_admin("embucket", "embucket").await.unwrap();
        s.create_account("acme", "acmeadmin", "pw1").await.unwrap();
        let scope = Arc::clone(&s).account_scope("acme").await.unwrap().unwrap();
        scope.create_role("r1").await.unwrap();
        scope.create_share("vendor").await.unwrap();
        scope.grant_to_share(ShareGrant {
            account: "acme".into(),
            share: "vendor".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("db".into()),
        })
        .await
        .unwrap();
        scope.grant(Grant {
            account: "acme".into(),
            role: "r1".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "s".into(), "t".into()),
        })
        .await
        .unwrap();

        s.drop_account("acme").await.unwrap();
        assert!(Arc::clone(&s).account_scope("acme").await.unwrap().is_none());
        // Default account untouched.
        assert!(s.authenticate("embucket", "embucket").await.unwrap().is_some());
        // Replay: no acme records remain in the journal.
        let s2 = InMemoryAccountStore::new_with_journal(journal).await.unwrap();
        assert!(s2.get_role("r1").await.unwrap().is_none());
        assert!(s2.get_share("vendor").await.unwrap().is_none());
        assert!(s2.authenticate("acmeadmin", "pw1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scoped_shares_are_account_local() {
        let s = Arc::new(store());
        s.create_account("acme", "acmeadmin", "pw1").await.unwrap();
        let acme = Arc::clone(&s).account_scope("acme").await.unwrap().unwrap();
        // Same share name in both accounts.
        s.create_share("vendor").await.unwrap();
        acme.create_share("vendor").await.unwrap();
        assert_eq!(s.list_shares().await.unwrap().len(), 1);
        assert_eq!(acme.list_shares().await.unwrap().len(), 1);
        // Grants made in one account are invisible in the other.
        acme.grant_to_share(ShareGrant {
            account: "acme".into(),
            share: "vendor".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("acmedb".into()),
        })
        .await
        .unwrap();
        assert_eq!(acme.share_grants("vendor").await.unwrap().len(), 1);
        assert!(s.share_grants("vendor").await.unwrap().is_empty());
        // Dropping the acme share does not touch the default one.
        acme.drop_share("vendor").await.unwrap();
        assert!(s.get_share("vendor").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn journal_replays_accounts_and_scopes() {
        let journal = Arc::new(MockJournal::default());
        let s = Arc::new(
            InMemoryAccountStore::new_with_journal(journal.clone())
                .await
                .unwrap(),
        );
        s.ensure_admin("embucket", "embucket").await.unwrap();
        s.create_account("acme", "acmeadmin", "pw1").await.unwrap();
        // "Restart": the account and its admin survive replay.
        let s2 = Arc::new(InMemoryAccountStore::new_with_journal(journal).await.unwrap());
        let scope = Arc::clone(&s2).account_scope("acme").await.unwrap().unwrap();
        assert!(scope.authenticate("acmeadmin", "pw1").await.unwrap().is_some());
        assert!(scope.get_role(ACCOUNTADMIN).await.unwrap().is_some());
        // Same-named user in default account stays separate.
        assert!(s2.authenticate("acmeadmin", "pw1").await.unwrap().is_none());
    }
}
