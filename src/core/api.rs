//! Datasets and storage API actions and helpers.
//!
//! For overall architecture, see [ARCHITECTURE.md](https://gitlab.com/tangram-vision-oss/bolster/-/blob/main/ARCHITECTURE.md)

pub(crate) mod datasets;
pub(crate) mod storage;

use std::time::Duration;

use anyhow::{bail, Context, Error, Result};
use log::debug;
use reqwest::{header, Response, StatusCode, Url};

/// Configuration for interacting with the datasets database.
pub struct DatabaseApiConfig {
    /// URL endpoint
    pub base_url: Url,
    /// Storage bucket (group id)
    pub bucket: String,
    /// HTTP client
    pub client: reqwest::Client,
}

impl DatabaseApiConfig {
    /// Configure HTTP client with endpoint, auth, and timeout.
    pub fn new_with_params(
        base_url: Url,
        bucket: String,
        bearer_access_token: String,
        timeout: u64,
    ) -> Result<Self> {
        let user_agent = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),);
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", bearer_access_token))?,
        );
        headers.insert(
            header::HeaderName::from_static("apikey"),
            // TODO: move this key to config? it has no expiry and is shared for everyone, which is kinda annoying/useless
            header::HeaderValue::from_str("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiJ9.ZopqoUt20nEV9cklpv9e3yw3PVyZLmKs5qLD6nGL1SI")?,
        );
        headers.insert(
            // NOTE: supabase/storage-api throws error if it doesn't receive a content-type
            header::CONTENT_TYPE,
            header::HeaderValue::from_str("application/octet-stream")?,
        );
        headers.insert(
            "Prefer",
            header::HeaderValue::from_str("return=representation")?,
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(user_agent)
                .default_headers(headers)
                .timeout(Duration::from_secs(timeout))
                .build()?,
            base_url,
            bucket,
        })
    }

    /// Configure HTTP client with endpoint, auth, and default 30-second timeout;
    pub fn new(base_url: Url, bucket: String, bearer_access_token: String) -> Result<Self> {
        let timeout = 30;
        Self::new_with_params(base_url, bucket, bearer_access_token, timeout)
    }
}

/// Responses with any of these [StatusCode]s show extra detail.
const ERROR_STATUSES_TO_SHOW_DETAIL: [StatusCode; 3] = [
    StatusCode::BAD_REQUEST,
    StatusCode::UNAUTHORIZED,
    StatusCode::FORBIDDEN,
];

/// Returns response json or an error with extra context/detail.
///
/// For responses with a status code in [ERROR_STATUSES_TO_SHOW_DETAIL], return
/// an error message that includes contents of "message", "detail", and "hint"
/// fields in the API response, if they're provided. This will be used to inform
/// users if they're providing bad input to the API or if a particular API
/// endpoint is disabled/retired (and the user should upgrade to a newer version
/// of bolster).
pub async fn check_response(response: Response) -> Result<serde_json::Value> {
    let status = response.status();
    debug!("check_response status: {}", status);
    let status_maybe_err = response.error_for_status_ref();
    if status_maybe_err.is_ok() {
        let content = response
            .json()
            .await
            .with_context(|| "JSON from API was malformed.");
        debug!("check_response content: {:?}", content);
        let content = content?;
        return Ok(content);
    }

    let status_err = status_maybe_err.unwrap_err();
    if status_err.status().is_some()
        && ERROR_STATUSES_TO_SHOW_DETAIL.contains(&status_err.status().unwrap())
    {
        response.json::<serde_json::Value>().await.map(|js| {
            // Build up error to show user from error message and any message,
            // detail, and hint fields that are populated.
            let mut err_msg = format!("{}", status_err);
            if let Some(Some(msg)) = js.get("message").map(|v| v.as_str()) {
                err_msg.push_str(&format!("\n\tMessage: {}", msg))
            }
            if let Some(Some(details)) = js.get("details").map(|v| v.as_str()) {
                err_msg.push_str(&format!("\n\tDetails: {}", details))
            }
            if let Some(Some(hint)) = js.get("hint").map(|v| v.as_str()) {
                err_msg.push_str(&format!("\n\tHint: {}", hint))
            }
            bail!(err_msg);
        })?
    } else {
        Err(Error::new(status_err))
    }
}
