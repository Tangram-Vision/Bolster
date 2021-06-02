//! Interact with the datasets database.
//!
//! The datasets database stores datasets, their files, and associated metadata.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use log::debug;
use reqwest::{header, Url};
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
    /// Configure HTTP client with auth, user-agent, and headers.
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
#[derive(Debug)]
pub struct DatasetGetRequest {
    /// Filter to a specific dataset
    pub dataset_id: Option<Uuid>,
    /// Filter to a specific device/robot/installation
    pub device_id: Option<String>,
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
    // - https://gitlab.com/tangram-vision-oss/bolster/-/issues/1
    // - https://gitlab.com/tangram-vision-oss/bolster/-/issues/4
}

impl Default for DatasetGetRequest {
    fn default() -> Self {
        Self {
            dataset_id: None,
            device_id: None,
            before_date: None,
            after_date: None,
            order: None,
            limit: None,
            offset: None,
        }
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
    if let Some(device_id) = &params.device_id {
        req_builder = req_builder.query(&[("device_id", format!("eq.{}", device_id))]);
    }
    if let Some(before_date) = &params.before_date {
        req_builder = req_builder.query(&[("created_date", format!("lt.{}", before_date))]);
    }
    if let Some(after_date) = &params.after_date {
        req_builder = req_builder.query(&[("created_date", format!("gte.{}", after_date))]);
    }
    // TODO: Implement metadata CLI input
    // Related to
    // - https://gitlab.com/tangram-vision-oss/bolster/-/issues/1
    // - https://gitlab.com/tangram-vision-oss/bolster/-/issues/4

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
    response.error_for_status_ref()?;

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("content: {}", content);

    let datasets: Vec<Dataset> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
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
    device_id: String,
    metadata: serde_json::Value,
) -> Result<DatasetNoFiles> {
    debug!("Building post request for: {} {:?}", device_id, metadata);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("datasets");
    let mut req_builder = client.post(api_url.as_str());

    let req_body = json!({
        "device_id": device_id,
        "metadata": metadata,
    });
    req_builder = req_builder.json(&req_body);

    let response = req_builder.send().await?;
    response.error_for_status_ref()?;

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("content: {}", content);

    let mut datasets: Vec<DatasetNoFiles> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
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
    response.error_for_status_ref()?;

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("content: {}", content);

    let files: Vec<UploadedFile> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Files API was malformed: {}", &content))?;
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
    response.error_for_status_ref()?;
    // TODO: Add context to 409 response (dataset doesn't exist) OR validate it
    // does before uploading to storage provider.

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("response content: {}", content);

    let mut uploaded_files: Vec<UploadedFile> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Files API was malformed: {}", &content))?;
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
    });
    req_builder = req_builder.json(&req_body);

    let response = req_builder.send().await?;
    response.error_for_status_ref()?;

    debug!("status: {}", response.status());
    let content = response.text().await?;
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
                    "device_id": "robot-1",
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
                    "device_id": "robot-1",
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
        let downcast = result.downcast_ref::<serde_json::Error>().unwrap();

        mock.assert();
        assert_eq!(downcast.classify(), serde_json::error::Category::Syntax);
        assert!(result
            .to_string()
            .contains("JSON from Datasets API was malformed: this isn't actually json"));
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
        let downcast = result.downcast_ref::<reqwest::Error>().unwrap();

        mock.assert();
        assert_eq!(
            downcast.status().unwrap(),
            reqwest::StatusCode::UNAUTHORIZED
        );
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
                .body(r#"{"dataset_id":"afd56ecf-9d87-4053-8c80-0d924f06da52"}"#)
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

        datasets_notify_upload_complete(&config, dataset_id)
            .await
            .unwrap();

        mock.assert();
    }
}
