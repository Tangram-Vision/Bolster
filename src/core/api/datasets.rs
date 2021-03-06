//! Interact with the datasets database.
//!
//! The datasets database stores datasets, their files, and associated metadata.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::NaiveDate;
use log::debug;
use reqwest::{header, Response, StatusCode, Url};
use serde_json::json;
use strum_macros::{Display, EnumString, EnumVariantNames};
use uuid::Uuid;

use crate::core::models::{Dataset, DatasetNoFiles, UploadedFile};

/// Configuration for interacting with the datasets database.
pub struct DatabaseApiConfig {
    /// URL endpoint
    pub base_url: Url,
    /// HTTP client
    pub client: reqwest::Client,
}

impl DatabaseApiConfig {
    /// Configure HTTP client with endpoint, auth, and timeout.
    pub fn new_with_params(
        base_url: Url,
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
        })
    }

    /// Configure HTTP client with endpoint, auth, and default 30-second timeout;
    pub fn new(base_url: Url, bearer_access_token: String) -> Result<Self> {
        let timeout = 30;
        Self::new_with_params(base_url, bearer_access_token, timeout)
    }
}

/// Available dataset sorting options
#[derive(EnumString, EnumVariantNames, Display, Debug)]
pub enum DatasetOrdering {
    /// Sort by dataset creation date, ascending (i.e. oldest first)
    #[strum(serialize = "created_date.asc")]
    CreatedDateAsc,
    /// Sort by dataset creation date, descending (i.e. most recent first)
    #[strum(serialize = "created_date.desc")]
    CreatedDateDesc,
}

impl DatasetOrdering {
    /// Translates between CLI sorting option value (e.g. "date") and database
    /// column (e.g. "created_date"), if necessary.
    pub fn to_database_field(&self) -> String {
        self.to_string()
    }
}

/// Options for filtering dataset list query.
#[derive(Debug, Default)]
pub struct DatasetGetRequest {
    /// Filter to a specific dataset
    pub dataset_id: Option<Uuid>,
    /// Filter to a specific system/device/robot/installation
    pub system_id: Option<String>,
    /// Filter to datasets before a date
    pub before_date: Option<NaiveDate>,
    /// Filter to datasets after a date
    pub after_date: Option<NaiveDate>,
    /// Order query results by a field (e.g. created_date) and direction (e.g.
    /// ascending).
    pub order: Option<DatasetOrdering>,
    /// Number of datasets to show (default=20, max=100).
    pub limit: Option<usize>,
    /// Skip N results (for pagination).
    ///
    /// Warning: Results may shift between subsequent bolster invocations if new
    /// datasets are being added at the same time.
    pub offset: Option<usize>,
    // TODO: Implement metadata CLI input
    // Related to
    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/1
    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/4
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

/// Get a list of datasets and their files.
///
/// # Errors
///
/// Returns an error if the datasets server returns a non-200 response (e.g. if
/// auth credentials are invalid, if server is unreachable) or if the returned
/// data is malformed (e.g. not json).
pub async fn datasets_get(
    configuration: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    debug!("building get request for: {:?}", params);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("datasets");
    api_url.set_query(Some("select=*,files(*)"));
    let mut req_builder = client.get(api_url.as_str());

    if let Some(dataset_id) = &params.dataset_id {
        req_builder = req_builder.query(&[("dataset_id", format!("eq.{}", dataset_id))]);
    }
    if let Some(system_id) = &params.system_id {
        req_builder = req_builder.query(&[("system_id", format!("eq.{}", system_id))]);
    }
    if let Some(before_date) = &params.before_date {
        req_builder = req_builder.query(&[("created_date", format!("lt.{}", before_date))]);
    }
    if let Some(after_date) = &params.after_date {
        req_builder = req_builder.query(&[("created_date", format!("gte.{}", after_date))]);
    }
    // TODO: Implement metadata CLI input
    // Related to
    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/1
    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/4

    if let Some(order) = &params.order {
        req_builder = req_builder.query(&[("order", order.to_database_field())]);
    }
    if let Some(limit) = &params.limit {
        req_builder = req_builder.query(&[("limit", limit)]);
    }
    if let Some(offset) = &params.offset {
        req_builder = req_builder.query(&[("offset", offset)]);
    }

    let response = req_builder.send().await?;

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("content: {}", content);

    let datasets: Vec<Dataset> = serde_json::from_value(content.clone())
        .with_context(|| format!("JSON from Datasets API was malformed: {}", content))?;
    Ok(datasets)
}

/// Create a new dataset in the datasets database.
///
/// The returned dataset contains the dataset's id, which should be recorded to
/// query for this dataset or download its files in the future.
///
/// # Errors
///
/// Returns an error if the datasets server returns a non-200 response (e.g. if
/// auth credentials are invalid, if server is unreachable) or if the returned
/// data is malformed (e.g. not json).
pub async fn datasets_post(
    configuration: &DatabaseApiConfig,
    system_id: String,
    metadata: serde_json::Value,
) -> Result<DatasetNoFiles> {
    debug!("Building post request for: {} {:?}", system_id, metadata);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("datasets");
    let mut req_builder = client.post(api_url.as_str());

    let req_body = json!({
        "system_id": system_id,
        "metadata": metadata,
    });
    req_builder = req_builder.json(&req_body);

    let response = req_builder.send().await?;

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("content: {}", content);

    let mut datasets: Vec<DatasetNoFiles> = serde_json::from_value(content.clone())
        .with_context(|| format!("JSON from Datasets API was malformed: {}", content))?;
    // PostgREST resturns a list, even when only a single object is expected
    // https://postgrest.org/en/v7.0.0/api.html#singular-or-plural
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for newly-created Dataset!"))
}

/// Get a list of files in a specified dataset, optionally filtered by
/// prefix(es).
///
/// # Errors
///
/// Returns an error if the datasets server returns a non-200 response (e.g. if
/// auth credentials are invalid, if server is unreachable) or if the returned
/// data is malformed (e.g. not json).
pub async fn files_get(
    configuration: &DatabaseApiConfig,
    dataset_id: Uuid,
    prefixes: Vec<String>,
) -> Result<Vec<UploadedFile>> {
    debug!(
        "building files get request for: {} {:?}",
        dataset_id, prefixes
    );
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("files");
    let req_builder = client.get(api_url.as_str());

    let req_builder = req_builder.query(&[("dataset_id", format!("eq.{}", dataset_id))]);

    // Example query strings:
    // bolster.tangramvision.com/files/?dataset_id={dataset-uuid}
    // bolster.tangramvision.com/files/?dataset_id={dataset-uuid}&or=(filepath.ilike.{prefix}*)
    // bolster.tangramvision.com/files/?dataset_id={dataset-uuid}&or=(filepath.ilike.{prefix}*,filepath.ilike.{prefix2}*,...)
    let req_builder = if prefixes.is_empty() {
        req_builder
    } else {
        req_builder.query(&[(
            "or",
            format!(
                "({})",
                prefixes
                    .into_iter()
                    .map(|s| format!("filepath.ilike.{}*", s))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        )])
    };

    let response = req_builder.send().await?;

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("content: {}", content);

    let files: Vec<UploadedFile> = serde_json::from_value(content.clone())
        .with_context(|| format!("JSON from Files API was malformed: {}", content))?;
    Ok(files)
}

/// Create a new file in a specified dataset.
///
/// # Errors
///
/// Returns an error if the datasets server returns a non-200 response (e.g. if
/// auth credentials are invalid, if server is unreachable) or if the returned
/// data is malformed (e.g. not json).
pub async fn files_post(
    configuration: &DatabaseApiConfig,
    dataset_id: Uuid,
    url: &Url,
    filesize: usize,
    version: String,
    metadata: serde_json::Value,
) -> Result<UploadedFile> {
    debug!("building files post request for: {} {}", dataset_id, url);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("files");
    let mut req_builder = client.post(api_url.as_str());

    let req_body = json!({
        "dataset_id": dataset_id,
        "url": url,
        "filesize": filesize,
        "version": version,
        "metadata": metadata,
    });
    req_builder = req_builder.json(&req_body);

    let response = req_builder.send().await?;
    // TODO: Add context to 409 response (dataset doesn't exist) OR validate it
    // does before uploading to storage provider.

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("response content: {}", content);

    let mut uploaded_files: Vec<UploadedFile> = serde_json::from_value(content.clone())
        .with_context(|| format!("JSON from Files API was malformed: {}", content))?;
    uploaded_files
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for updated File!"))
}

/// Notify backend that uploading a dataset is complete.
///
/// This API call may trigger backend processing or notifications.
///
/// # Errors
///
/// Returns an error if the datasets server returns a non-200 response (e.g. if
/// auth credentials are invalid, if server is unreachable).
pub async fn datasets_notify_upload_complete(
    configuration: &DatabaseApiConfig,
    dataset_id: Uuid,
    plex_file_id: Uuid,
    object_space_file_id: Uuid,
) -> Result<()> {
    debug!(
        "Building datasets_notify_upload_complete post request for: {}",
        dataset_id
    );
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("rpc/dataset_upload_complete");
    let mut req_builder = client.post(api_url.as_str());

    let req_body = json!({
        "dataset_id": dataset_id,
        "plex_file_id": plex_file_id,
        "object_space_file_id": object_space_file_id,
    });
    req_builder = req_builder.json(&req_body);

    let response = req_builder.send().await?;

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("content: {}", content);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use httpmock::{
        Method::{GET, POST},
        MockServer,
    };

    use super::*;

    #[tokio::test]
    async fn test_check_response_200() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": "b",
                    "hint": "c",
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        check_response(response)
            .await
            .expect("200 response should be Ok");

        mock.assert();
    }

    #[tokio::test]
    async fn test_check_response_400_message_details_hint() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(400)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": "b",
                    "hint": "c",
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        let error = check_response(response)
            .await
            .expect_err("400 response should be Err");

        mock.assert();
        assert!(format!("{}", error).contains("Message: a"));
        assert!(format!("{}", error).contains("Details: b"));
        assert!(format!("{}", error).contains("Hint: c"));
    }

    #[tokio::test]
    async fn test_check_response_400_message_only() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(400)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": null,
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        let error = check_response(response)
            .await
            .expect_err("400 response should be Err");

        mock.assert();
        println!("{}", error);
        assert!(format!("{}", error).contains("Message: a"));
        assert!(!format!("{}", error).contains("Details"));
        assert!(!format!("{}", error).contains("Hint"));
    }

    #[tokio::test]
    async fn test_check_response_401_message_details_hint() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(401)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": "b",
                    "hint": "c",
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        let error = check_response(response)
            .await
            .expect_err("401 response should be Err");

        mock.assert();
        assert!(format!("{}", error).contains("Message: a"));
        assert!(format!("{}", error).contains("Details: b"));
        assert!(format!("{}", error).contains("Hint: c"));
    }

    #[tokio::test]
    async fn test_check_response_403_message_details_hint() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(403)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": "b",
                    "hint": "c",
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        let error = check_response(response)
            .await
            .expect_err("403 response should be Err");

        mock.assert();
        assert!(format!("{}", error).contains("Message: a"));
        assert!(format!("{}", error).contains("Details: b"));
        assert!(format!("{}", error).contains("Hint: c"));
    }

    #[tokio::test]
    async fn test_check_response_500_has_no_details() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET);
            then.status(500)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "message": "a",
                    "details": "b",
                    "hint": "c",
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let req_builder = config.client.get(config.base_url.clone().as_str());
        let response = req_builder.send().await.unwrap();

        let error = check_response(response)
            .await
            .expect_err("500 response should be Err");

        mock.assert();
        assert!(!format!("{}", error).contains("Message"));
        assert!(!format!("{}", error).contains("Details"));
        assert!(!format!("{}", error).contains("Hint"));
    }

    #[tokio::test]
    async fn test_datasets_get_success() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .query_param("select", "*,files(*)")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "dataset_id": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    "system_id": "robot-1",
                    "metadata": {
                        "description": "Test"
                    },
                    "files": [],
                }]));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).await.unwrap();

        mock.assert();
        assert_eq!(
            result[0].dataset_id,
            Uuid::parse_str("afd56ecf-9d87-4053-8c80-0d924f06da52").unwrap()
        );
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_datasets_get_query_params() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .query_param("created_date", "gte.2021-01-01")
                .query_param("order", "created_date.desc")
                .query_param("limit", "17")
                .query_param("select", "*,files(*)")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "dataset_id": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    "system_id": "robot-1",
                    "metadata": {
                        "description": "Test"
                    },
                    "files": [],
                }]));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let params = DatasetGetRequest {
            after_date: Some(NaiveDate::from_str("2021-01-01").unwrap()),
            order: Some(DatasetOrdering::CreatedDateDesc),
            limit: Some(17),
            ..Default::default()
        };

        let result = datasets_get(&config, &params).await.unwrap();

        mock.assert();
        assert_eq!(
            result[0].dataset_id,
            Uuid::parse_str("afd56ecf-9d87-4053-8c80-0d924f06da52").unwrap()
        );
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_datasets_get_wrong_structure_json() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!({
                    "dataset_id": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    "url": "https://example.com/afd56ecf-9d87-4053-8c80-0d924f06da52/hello.txt",
                    "metadata": {
                        "description": "Test"
                    }
                }));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params)
            .await
            .expect_err("Expected json parsing error");
        let downcast = result.downcast_ref::<serde_json::Error>().unwrap();

        mock.assert();
        // Expected json structure is a list of maps, not a single map
        assert_eq!(downcast.classify(), serde_json::error::Category::Data);
        assert!(result
            .to_string()
            .contains("JSON from Datasets API was malformed: {"));
    }

    #[tokio::test]
    async fn test_datasets_get_malformed_json() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .body("this isn't actually json");
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params)
            .await
            .expect_err("Expected json parsing error");
        println!("result: {:?}", result);
        let downcast = result.downcast_ref::<reqwest::Error>().unwrap();

        mock.assert();
        assert!(downcast.is_decode());
        assert!(result.to_string().contains("JSON from API was malformed"));
    }

    #[tokio::test]
    async fn test_datasets_get_401_response() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(401)
                .header("Content-Type", "application/json")
                .json_body(json!({"message": "JWSError JWSInvalidSignature"}));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params)
            .await
            .expect_err("Expected status code error");

        mock.assert();
        assert!(result
            .to_string()
            .contains("HTTP status client error (401 Unauthorized) for url"));
        // Could add `.map_err` and `.with_context` to `.error_for_status` calls
        // to prompt user to check their API key for 401 responses.
    }

    #[tokio::test]
    async fn test_datasets_get_timeout() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .delay(Duration::from_millis(1200))
                .body("Should never see this due to timeout");
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            1,
        )
        .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params)
            .await
            .expect_err("Expected timeout error");
        let downcast = result.downcast_ref::<reqwest::Error>().unwrap();

        mock.assert();
        assert!(downcast.is_timeout());
        assert!(result.to_string().contains("operation timed out"));
    }

    #[tokio::test]
    async fn test_datasets_notify_upload_complete() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .header("Authorization", "Bearer TEST-TOKEN")
                .json_body(json!({"dataset_id":"afd56ecf-9d87-4053-8c80-0d924f06da52","plex_file_id":"bfd56ecf-9d87-4053-8c80-0d924f06da52","object_space_file_id":"cb0daadc-554d-49d7-ba77-967754b15667"}))
                .path("/rpc/dataset_upload_complete");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "status": "ok",
                }]));
        });

        let config = DatabaseApiConfig::new_with_params(
            Url::parse(&server.base_url()).unwrap(),
            "TEST-TOKEN".to_owned(),
            10,
        )
        .unwrap();
        let dataset_id = Uuid::parse_str("afd56ecf-9d87-4053-8c80-0d924f06da52").unwrap();
        let plex_file_id = Uuid::parse_str("bfd56ecf-9d87-4053-8c80-0d924f06da52").unwrap();
        let object_space_file_id = Uuid::parse_str("cb0daadc-554d-49d7-ba77-967754b15667").unwrap();

        datasets_notify_upload_complete(&config, dataset_id, plex_file_id, object_space_file_id)
            .await
            .unwrap();

        mock.assert();
    }
}
