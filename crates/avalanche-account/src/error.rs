//! Account-plane errors (design §17).

use snafu::Location;

#[derive(snafu::Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Role {role} already exists"))]
    RoleAlreadyExists {
        role: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Role {role} does not exist"))]
    RoleNotFound {
        role: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("User {user} already exists"))]
    UserAlreadyExists {
        user: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("User {user} does not exist"))]
    UserNotFound {
        user: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Share {share} already exists"))]
    ShareAlreadyExists {
        share: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Share {share} does not exist"))]
    ShareNotFound {
        share: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{message}"))]
    GrantNotFound {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{message}"))]
    NotAllowed {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{message}"))]
    InvalidInput {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    #[must_use]
    pub fn is_role_already_exists(&self) -> bool {
        matches!(self, Self::RoleAlreadyExists { .. })
    }

    #[must_use]
    pub fn is_user_already_exists(&self) -> bool {
        matches!(self, Self::UserAlreadyExists { .. })
    }

    #[must_use]
    pub fn is_share_already_exists(&self) -> bool {
        matches!(self, Self::ShareAlreadyExists { .. })
    }
}