//! Account store (design §17, M10.1): users, roles, grants.
//!
//! `InMemoryAccountStore` keeps process-lifetime state; with a journal
//! (M17.3.1, design §23) it is also durable — every mutation is mirrored
//! write-through and bootstrap replays the journal.

use crate::error::{self as account_error, Result};
use crate::models::{
    AccountObject, Grant, LoginEvent, Privilege, Role, RoleRole, Share, ShareGrant, User, UserRole,
    ACCOUNTADMIN,
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
    users: HashMap<String, User>,
    roles: HashMap<String, Role>,
    grants: Vec<Grant>,
    user_roles: Vec<UserRole>,
    role_roles: Vec<RoleRole>,
    shares: HashMap<String, Share>,
    share_grants: Vec<ShareGrant>,
    logins: Vec<LoginEvent>,
}

/// In-memory [`AccountStore`].
#[derive(Debug, Default)]
pub struct InMemoryAccountStore {
    state: RwLock<StoreState>,
    /// Write-through journal (M17.3.1): every mutation is mirrored to the
    /// durable backend before the in-memory state changes; `None` for the
    /// plain process-lifetime store.
    journal: Option<Arc<dyn crate::journal::AccountJournal>>,
    /// Insertion sequence for login events (stable journal keys + ordering).
    login_seq: AtomicU64,
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
                    crate::journal::AccountRecord::User(u) => {
                        state.users.insert(key(&u.name), u);
                    }
                    crate::journal::AccountRecord::Role(r) => {
                        state.roles.insert(key(&r.name), r);
                    }
                    crate::journal::AccountRecord::UserRole(ur) => {
                        state.user_roles.push(ur);
                    }
                    crate::journal::AccountRecord::RoleRole(rr) => {
                        state.role_roles.push(rr);
                    }
                    crate::journal::AccountRecord::Grant(g) => state.grants.push(g),
                    crate::journal::AccountRecord::Share(s) => {
                        state.shares.insert(key(&s.name), s);
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
            state: RwLock::new(state),
            journal: Some(journal),
            login_seq: AtomicU64::new(0),
        })
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
            name: ACCOUNTADMIN.to_string(),
        };
        let user = User {
            name: name.to_string(),
            login_name: name.to_string(),
            password: password.to_string(),
            default_role: Some(ACCOUNTADMIN.to_string()),
            tenant: None,
            disabled: false,
        };
        let mut state = StoreState::default();
        state.roles.insert(role.name.to_ascii_lowercase(), role);
        state.users.insert(user.name.to_ascii_lowercase(), user);
        state.user_roles.push(UserRole {
            user: name.to_string(),
            role: ACCOUNTADMIN.to_string(),
        });
        Self {
            state: RwLock::new(state),
            journal: None,
            login_seq: AtomicU64::new(0),
        }
    }
}

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[async_trait]
impl AccountStore for InMemoryAccountStore {
    async fn create_role(&self, name: &str) -> Result<Role> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let role = Role {
            name: name.to_string(),
        };
        if state.roles.contains_key(&key(name)) {
            return account_error::RoleAlreadyExistsSnafu { role: name.to_string() }.fail();
        }
        state.roles.insert(key(name), role.clone());
        self.journal_put(&crate::journal::AccountRecord::Role(role.clone()))
            .await?;
        Ok(role)
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.roles.remove(&key(name)).is_none() {
            return account_error::RoleNotFoundSnafu { role: name.to_string() }.fail();
        }
        // Journal the cascades before mutating (keys computed from the
        // records being removed).
        let removed_grants: Vec<Grant> = state
            .grants
            .iter()
            .filter(|g| key(&g.role).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        let removed_links: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| key(&ur.role).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        // Role-role grants vanish with either endpoint (M17.3).
        let removed_role_links: Vec<RoleRole> = state
            .role_roles
            .iter()
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
            name: name.to_string(),
        }))
        .await?;
        state.grants.retain(|g| !key(&g.role).eq_ignore_ascii_case(&key(name)));
        state
            .user_roles
            .retain(|ur| !key(&ur.role).eq_ignore_ascii_case(&key(name)));
        state
            .role_roles
            .retain(|rr| !(key(&rr.child) == key(name) || key(&rr.parent) == key(name)));
        Ok(())
    }

    async fn get_role(&self, name: &str) -> Result<Option<Role>> {
        let state = self.state.read().await;
        Ok(state.roles.get(&key(name)).cloned())
    }

    async fn list_roles(&self) -> Result<Vec<Role>> {
        let state = self.state.read().await;
        Ok(state.roles.values().cloned().collect())
    }

    async fn create_user(&self, user: User) -> Result<User> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.users.contains_key(&key(&user.name)) {
            return account_error::UserAlreadyExistsSnafu { user: user.name.clone() }.fail();
        }
        state.users.insert(key(&user.name), user.clone());
        self.journal_put(&crate::journal::AccountRecord::User(user.clone()))
            .await?;
        Ok(user)
    }

    async fn drop_user(&self, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.users.remove(&key(name)).is_none() {
            return account_error::UserNotFoundSnafu { user: name.to_string() }.fail();
        }
        let removed_links: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| key(&ur.user).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        for ur in &removed_links {
            self.journal_delete(&crate::journal::AccountRecord::UserRole(ur.clone()))
                .await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::User(User {
            name: name.to_string(),
            login_name: String::new(),
            password: String::new(),
            default_role: None,
            tenant: None,
            disabled: false,
        }))
        .await?;
        state
            .user_roles
            .retain(|ur| !key(&ur.user).eq_ignore_ascii_case(&key(name)));
        Ok(())
    }

    async fn get_user(&self, login_name: &str) -> Result<Option<User>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .find(|u| key(&u.login_name) == key(login_name))
            .cloned())
    }

    async fn list_users(&self) -> Result<Vec<User>> {
        let state = self.state.read().await;
        Ok(state.users.values().cloned().collect())
    }

    async fn grant_role_to_user(&self, user: &str, role: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.users.contains_key(&key(user)) {
            return account_error::UserNotFoundSnafu { user: user.to_string() }.fail();
        }
        if !state.roles.contains_key(&key(role)) {
            return account_error::RoleNotFoundSnafu { role: role.to_string() }.fail();
        }
        let link = UserRole {
            user: user.to_string(),
            role: role.to_string(),
        };
        let exists = state
            .user_roles
            .iter()
            .any(|ur| key(&ur.user) == key(user) && key(&ur.role) == key(role));
        if !exists {
            state.user_roles.push(link.clone());
            self.journal_put(&crate::journal::AccountRecord::UserRole(link))
                .await?;
        }
        Ok(())
    }

    async fn revoke_role_from_user(&self, user: &str, role: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<UserRole> = state
            .user_roles
            .iter()
            .filter(|ur| key(&ur.user) == key(user) && key(&ur.role) == key(role))
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
        state
            .user_roles
            .retain(|ur| !(key(&ur.user) == key(user) && key(&ur.role) == key(role)));
        Ok(())
    }

    async fn user_roles(&self, user: &str) -> Result<Vec<String>> {
        let state = self.state.read().await;
        Ok(state
            .user_roles
            .iter()
            .filter(|ur| key(&ur.user) == key(user))
            .map(|ur| ur.role.clone())
            .collect())
    }

    async fn grant_role_to_role(&self, child: &str, parent: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.roles.contains_key(&key(child)) {
            return account_error::RoleNotFoundSnafu { role: child.to_string() }.fail();
        }
        if !state.roles.contains_key(&key(parent)) {
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
        if role_graph_reachable(&state.role_roles, &key(child), &key(parent)) {
            return account_error::NotAllowedSnafu {
                message: format!(
                    "circular role hierarchy detected: role '{parent}' \
                     already inherits from '{child}'"
                ),
            }
            .fail();
        }
        let link = RoleRole {
            child: child.to_string(),
            parent: parent.to_string(),
        };
        let exists = state
            .role_roles
            .iter()
            .any(|rr| key(&rr.child) == key(child) && key(&rr.parent) == key(parent));
        if !exists {
            state.role_roles.push(link.clone());
            self.journal_put(&crate::journal::AccountRecord::RoleRole(link))
                .await?;
        }
        Ok(())
    }

    async fn revoke_role_from_role(&self, child: &str, parent: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<RoleRole> = state
            .role_roles
            .iter()
            .filter(|rr| key(&rr.child) == key(child) && key(&rr.parent) == key(parent))
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
        state
            .role_roles
            .retain(|rr| !(key(&rr.child) == key(child) && key(&rr.parent) == key(parent)));
        Ok(())
    }

    async fn role_children(&self, parent: &str) -> Result<Vec<String>> {
        let state = self.state.read().await;
        Ok(state
            .role_roles
            .iter()
            .filter(|rr| key(&rr.parent) == key(parent))
            .map(|rr| rr.child.clone())
            .collect())
    }

    async fn effective_roles(&self, roles: &[String]) -> Result<Vec<String>> {
        let state = self.state.read().await;
        // Resolve a lowercase key back to a display name: prefer the seed
        // (caller) casing, then the stored grant casing, then the key.
        let name_for = |k: &str| -> String {
            if let Some(r) = roles.iter().find(|r| key(r) == k) {
                return r.clone();
            }
            state
                .role_roles
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
            for rr in state.role_roles.iter().filter(|rr| key(&rr.parent) == next) {
                let ck = key(&rr.child);
                if !queue.contains(&ck) && !out.iter().any(|r| key(r) == ck) {
                    queue.push(ck);
                }
            }
        }
        Ok(out)
    }

    async fn grant(&self, grant: Grant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.roles.contains_key(&key(&grant.role)) {
            return account_error::RoleNotFoundSnafu {
                role: grant.role.clone(),
            }
            .fail();
        }
        let exists = state.grants.iter().any(|g| {
            key(&g.role) == key(&grant.role)
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

    async fn revoke(&self, grant: &Grant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<Grant> = state
            .grants
            .iter()
            .filter(|g| {
                key(&g.role) == key(&grant.role)
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
            !(key(&g.role) == key(&grant.role)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        Ok(())
    }

    async fn role_grants(&self, role: &str) -> Result<Vec<Grant>> {
        let state = self.state.read().await;
        Ok(state
            .grants
            .iter()
            .filter(|g| key(&g.role) == key(role))
            .cloned()
            .collect())
    }

    async fn share_grants(&self, share: &str) -> Result<Vec<ShareGrant>> {
        let state = self.state.read().await;
        if !state.shares.contains_key(&key(share)) {
            return account_error::ShareNotFoundSnafu { share: share.to_string() }.fail();
        }
        Ok(state
            .share_grants
            .iter()
            .filter(|g| key(&g.share) == key(share))
            .cloned()
            .collect())
    }

    async fn authenticate(&self, login_name: &str, password: &str) -> Result<Option<User>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .find(|u| key(&u.login_name) == key(login_name))
            .filter(|u| !u.disabled && u.password == password)
            .cloned())
    }

    async fn record_login(&self, event: LoginEvent) -> Result<()> {
        let mut event = event;
        event.sequence = self.login_seq.fetch_add(1, Ordering::Relaxed);
        self.journal_put(&crate::journal::AccountRecord::Login(event.clone()))
            .await?;
        self.state.write().await.logins.push(event);
        Ok(())
    }

    async fn list_logins(&self) -> Result<Vec<LoginEvent>> {
        let state = self.state.read().await;
        Ok(state.logins.clone())
    }

    async fn allowed(
        &self,
        role: &str,
        privilege: Privilege,
        object: &AccountObject,
    ) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.grants.iter().any(|g| {
            key(&g.role) == key(role) && g.privilege == privilege && g.object == *object
        }))
    }

    async fn create_share(&self, name: &str) -> Result<Share> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.contains_key(&key(name)) {
            return account_error::ShareAlreadyExistsSnafu { share: name.to_string() }.fail();
        }
        let share = Share {
            name: name.to_string(),
            kind: crate::models::ShareKind::Outbound,
            provider: None,
        };
        state.shares.insert(key(name), share.clone());
        self.journal_put(&crate::journal::AccountRecord::Share(share.clone()))
            .await?;
        Ok(share)
    }

    async fn create_inbound_share(&self, name: &str, provider: &str) -> Result<Share> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.contains_key(&key(name)) {
            return account_error::ShareAlreadyExistsSnafu { share: name.to_string() }.fail();
        }
        if !state.shares.contains_key(&key(provider)) {
            return account_error::ShareNotFoundSnafu {
                share: provider.to_string(),
            }
            .fail();
        }
        let share = Share {
            name: name.to_string(),
            kind: crate::models::ShareKind::Inbound,
            provider: Some(provider.to_string()),
        };
        state.shares.insert(key(name), share.clone());
        self.journal_put(&crate::journal::AccountRecord::Share(share.clone()))
            .await?;
        Ok(share)
    }

    async fn drop_share(&self, name: &str) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if state.shares.remove(&key(name)).is_none() {
            return account_error::ShareNotFoundSnafu { share: name.to_string() }.fail();
        }
        let removed: Vec<ShareGrant> = state
            .share_grants
            .iter()
            .filter(|g| key(&g.share).eq_ignore_ascii_case(&key(name)))
            .cloned()
            .collect();
        for g in &removed {
            self.journal_delete(&crate::journal::AccountRecord::ShareGrant(g.clone()))
                .await?;
        }
        self.journal_delete(&crate::journal::AccountRecord::Share(Share {
            name: name.to_string(),
            kind: crate::models::ShareKind::Outbound,
            provider: None,
        }))
        .await?;
        state
            .share_grants
            .retain(|g| !key(&g.share).eq_ignore_ascii_case(&key(name)));
        Ok(())
    }

    async fn get_share(&self, name: &str) -> Result<Option<Share>> {
        let state = self.state.read().await;
        Ok(state.shares.get(&key(name)).cloned())
    }

    async fn list_shares(&self) -> Result<Vec<Share>> {
        let state = self.state.read().await;
        Ok(state.shares.values().cloned().collect())
    }

    async fn grant_to_share(&self, grant: ShareGrant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        if !state.shares.contains_key(&key(&grant.share)) {
            return account_error::ShareNotFoundSnafu {
                share: grant.share.clone(),
            }
            .fail();
        }
        let exists = state.share_grants.iter().any(|g| {
            key(&g.share) == key(&grant.share)
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

    async fn revoke_from_share(&self, grant: &ShareGrant) -> Result<()> {
        bump_account_epoch();
        let mut state = self.state.write().await;
        let removed: Vec<ShareGrant> = state
            .share_grants
            .iter()
            .filter(|g| {
                key(&g.share) == key(&grant.share)
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
            !(key(&g.share) == key(&grant.share)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        Ok(())
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
            role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        assert!(s
            .revoke(&Grant {
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
            role: "analyst".into(),
            privilege: Privilege::Select,
            object: AccountObject::Table("db".into(), "public".into(), "t".into()),
        })
        .await
        .unwrap();
        s.create_share("vendor").await.unwrap();
        s.grant_to_share(ShareGrant {
            share: "vendor".into(),
            privilege: Privilege::Usage,
            object: AccountObject::Database("db".into()),
        })
        .await
        .unwrap();
        s.record_login(LoginEvent {
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
        let s = InMemoryAccountStore::new();
        let before = super::account_epoch();
        s.create_role("e1").await.unwrap();
        assert!(super::account_epoch() > before, "create_role bumps");
        s.record_login(LoginEvent {
            timestamp_ms: 0,
            sequence: 0,
            user_name: "e1".into(),
            client_app_id: None,
            client_ip: None,
            success: true,
        })
        .await
        .unwrap();
        assert_eq!(
            super::account_epoch(),
            before + 1,
            "record_login must not bump (no authorization effect)"
        );
        s.grant(Grant {
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
        let after_grant = super::account_epoch();
        assert!(after_grant > before + 1, "grant bumps");
        s.revoke(&Grant {
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
        assert!(super::account_epoch() > after_grant, "revoke bumps");
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
}