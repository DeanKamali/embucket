//! Account store (design §17, M10.1): users, roles, grants.
//!
//! `InMemoryAccountStore` keeps process-lifetime state — consistent with the
//! in-memory metastore. PG persistence of account records is an explicit
//! follow-up (state-store trait + tables), not M10 scope.

use crate::error::{self as account_error, Result};
use crate::models::{
    AccountObject, Grant, Privilege, Role, Share, ShareGrant, User, UserRole, ACCOUNTADMIN,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::RwLock;

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

    async fn grant(&self, grant: Grant) -> Result<()>;
    async fn revoke(&self, grant: &Grant) -> Result<()>;
    async fn role_grants(&self, role: &str) -> Result<Vec<Grant>>;

    /// Shares (design §21, M15.1): named grant bundles.
    async fn create_share(&self, name: &str) -> Result<Share>;
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
    shares: HashMap<String, Share>,
    share_grants: Vec<ShareGrant>,
}

/// In-memory [`AccountStore`].
#[derive(Debug, Default)]
pub struct InMemoryAccountStore {
    state: RwLock<StoreState>,
}

impl InMemoryAccountStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        }
    }
}

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[async_trait]
impl AccountStore for InMemoryAccountStore {
    async fn create_role(&self, name: &str) -> Result<Role> {
        let mut state = self.state.write().await;
        let role = Role {
            name: name.to_string(),
        };
        if state.roles.contains_key(&key(name)) {
            return account_error::RoleAlreadyExistsSnafu { role: name.to_string() }.fail();
        }
        state.roles.insert(key(name), role.clone());
        Ok(role)
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.roles.remove(&key(name)).is_none() {
            return account_error::RoleNotFoundSnafu { role: name.to_string() }.fail();
        }
        state.grants.retain(|g| !key(&g.role).eq_ignore_ascii_case(&key(name)));
        state
            .user_roles
            .retain(|ur| !key(&ur.role).eq_ignore_ascii_case(&key(name)));
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
        let mut state = self.state.write().await;
        if state.users.contains_key(&key(&user.name)) {
            return account_error::UserAlreadyExistsSnafu { user: user.name.clone() }.fail();
        }
        state.users.insert(key(&user.name), user.clone());
        Ok(user)
    }

    async fn drop_user(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.users.remove(&key(name)).is_none() {
            return account_error::UserNotFoundSnafu { user: name.to_string() }.fail();
        }
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
            state.user_roles.push(link);
        }
        Ok(())
    }

    async fn revoke_role_from_user(&self, user: &str, role: &str) -> Result<()> {
        let mut state = self.state.write().await;
        let before = state.user_roles.len();
        state
            .user_roles
            .retain(|ur| !(key(&ur.user) == key(user) && key(&ur.role) == key(role)));
        if state.user_roles.len() == before {
            return account_error::GrantNotFoundSnafu {
                message: format!("role '{role}' is not granted to user '{user}'"),
            }
            .fail();
        }
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

    async fn grant(&self, grant: Grant) -> Result<()> {
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
            state.grants.push(grant);
        }
        Ok(())
    }

    async fn revoke(&self, grant: &Grant) -> Result<()> {
        let mut state = self.state.write().await;
        let before = state.grants.len();
        state.grants.retain(|g| {
            !(key(&g.role) == key(&grant.role)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        if state.grants.len() == before {
            return account_error::GrantNotFoundSnafu {
                message: format!(
                    "privilege '{}' on {} '{}' is not granted to role '{}'",
                    grant.privilege, grant.object.kind(), grant.object, grant.role
                ),
            }
            .fail();
        }
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
        let mut state = self.state.write().await;
        if state.shares.contains_key(&key(name)) {
            return account_error::ShareAlreadyExistsSnafu { share: name.to_string() }.fail();
        }
        let share = Share {
            name: name.to_string(),
        };
        state.shares.insert(key(name), share.clone());
        Ok(share)
    }

    async fn drop_share(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.shares.remove(&key(name)).is_none() {
            return account_error::ShareNotFoundSnafu { share: name.to_string() }.fail();
        }
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
            state.share_grants.push(grant);
        }
        Ok(())
    }

    async fn revoke_from_share(&self, grant: &ShareGrant) -> Result<()> {
        let mut state = self.state.write().await;
        let before = state.share_grants.len();
        state.share_grants.retain(|g| {
            !(key(&g.share) == key(&grant.share)
                && g.privilege == grant.privilege
                && g.object == grant.object)
        });
        if state.share_grants.len() == before {
            return account_error::GrantNotFoundSnafu {
                message: format!(
                    "privilege '{}' on {} '{}' is not granted to share '{}'",
                    grant.privilege, grant.object.kind(), grant.object, grant.share
                ),
            }
            .fail();
        }
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
}