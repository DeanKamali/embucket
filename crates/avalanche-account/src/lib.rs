//! Account plane RBAC & tenancy for Avalanche (design §17, M10).
//!
//! - [`models`]: users, roles, privileges, account objects, grants.
//! - [`store`]: [`store::AccountStore`] trait + in-memory implementation.
//! - [`authz`]: per-session role materialization and the DataFusion
//!   catalog wrappers enforcing grants (deny = as-if-missing).

pub mod authz;
pub mod error;
pub mod models;
pub mod store;

pub use authz::{AuthzCatalogList, SessionAuthz};