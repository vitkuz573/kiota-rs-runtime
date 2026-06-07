mod anonymous;
mod api_key;
mod base_bearer_token;

pub use anonymous::AnonymousAuthenticationProvider;
pub use api_key::{ApiKeyAuthenticationProvider, ApiKeyLocation};
pub use base_bearer_token::BaseBearerTokenAuthenticationProvider;

use std::collections::HashMap;

use crate::request_information::RequestInformation;
use crate::KiotaError;

#[async_trait::async_trait]
pub trait AuthenticationProvider: Send + Sync {
    async fn authenticate_request(
        &self,
        request: &mut RequestInformation,
        additional_context: Option<&HashMap<String, String>>,
    ) -> Result<(), KiotaError>;
}

#[async_trait::async_trait]
pub trait AccessTokenProvider: Send + Sync {
    async fn get_authorization_token(
        &self,
        url: &url::Url,
        additional_context: Option<&HashMap<String, String>>,
    ) -> Result<Option<String>, KiotaError>;

    fn allowed_hosts_validator(&self) -> &AllowedHostsValidator;
}

#[derive(Clone, Debug)]
pub struct AllowedHostsValidator {
    allowed_hosts: std::collections::HashSet<String>,
}

impl AllowedHostsValidator {
    pub fn new(allowed_hosts: Vec<String>) -> Self {
        Self {
            allowed_hosts: allowed_hosts.into_iter().map(|h| h.to_lowercase()).collect(),
        }
    }

    pub fn is_url_host_valid(&self, url: &url::Url) -> bool {
        if self.allowed_hosts.is_empty() {
            return true;
        }
        url.host_str()
            .map(|h| self.allowed_hosts.contains(&h.to_lowercase()))
            .unwrap_or(false)
    }
}
