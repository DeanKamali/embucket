use crate::models::Auth;
use avalanche_account::store::AccountStore;
use executor::utils::DataSerializationFormat;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct RestApiConfig {
    pub auth: Auth,
    pub dbt_serialization_format: DataSerializationFormat,
    /// Account store (design §17, M10): the RBAC credential source. When
    /// absent, login falls back to the static demo credentials.
    pub account: Option<Arc<dyn AccountStore>>,
}

impl RestApiConfig {
    pub fn new(data_format: &str, jwt_secret: String) -> Result<Self, strum::ParseError> {
        Ok(Self {
            dbt_serialization_format: DataSerializationFormat::try_from(data_format)?,
            auth: Auth::new(jwt_secret),
            account: None,
        })
    }
    #[must_use]
    pub fn with_demo_credentials(mut self, demo_user: String, demo_password: String) -> Self {
        self.auth = Auth {
            demo_user,
            demo_password,
            ..self.auth
        };
        self
    }
    #[must_use]
    pub fn with_account(mut self, account: Arc<dyn AccountStore>) -> Self {
        self.account = Some(account);
        self
    }
}
