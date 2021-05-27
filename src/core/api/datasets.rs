// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use log::debug;
use reqwest::{header, Url};
use serde_json::json;
use strum_macros::{Display, EnumString, EnumVariantNames};
use uuid::Uuid;

use crate::core::models::{Dataset, DatasetNoFiles, UploadedFile};

pub struct DatabaseApiConfig {
    pub base_url: Url,
    pub client: reqwest::Client,
}

impl DatabaseApiConfig {
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

// Only allow a single sort key for now
#[derive(EnumString, EnumVariantNames, Display, Debug)]
pub enum DatasetOrdering {
    #[strum(serialize = "created_date.asc")]
    CreatedDateAsc,
    #[strum(serialize = "created_date.desc")]
    CreatedDateDesc,
}

impl DatasetOrdering {
    // For possible dataset ordering options where the CLI name (e.g. "creator")
    // doesn't match the API/database name (e.g. "creator_role"), translate
    // between them
    fn to_database_field(&self) -> String {
        match self {
            // DatasetOrdering::CreatorAsc => "creator_role.asc".to_owned(),
            // DatasetOrdering::CreatorDesc => "creator_role.desc".to_owned(),
            other => other.to_string(),
            // TODO: test order by creator
        }
    }
}

#[derive(Debug)]
pub struct DatasetGetRequest {
    pub dataset_id: Option<Uuid>,
    pub before_date: Option<NaiveDate>,
    pub after_date: Option<NaiveDate>,
    // TODO: implement metadata: Option<String>,
    pub order: Option<DatasetOrdering>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Default for DatasetGetRequest {
    fn default() -> Self {
        Self {
            dataset_id: None,
            before_date: None,
            after_date: None,
            order: None,
            limit: None,
            offset: None,
        }
    }
}

/*
pub fn datasets_patch(
    configuration: &DatabaseApiConfig,
    uuid: Uuid,
    new_url: &Url,
) -> Result<Dataset> {
    debug!("building patch request for: {}", uuid);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("datasets");
    let mut req_builder = client.patch(api_url.as_str());

    req_builder = req_builder.query(&[("uuid", format!("eq.{}", uuid.to_string()))]);

    let req_body = json!({ "url": new_url });
    req_builder = req_builder.json(&req_body);

    let request = req_builder.build()?;
    let response = client.execute(request)?.error_for_status()?;

    debug!("status: {}", response.status());
    let content = response.text()?;
    debug!("response content: {}", content);

    let mut datasets: Vec<Dataset> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for updated Dataset!"))
}
*/

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
    if let Some(before_date) = &params.before_date {
        req_builder = req_builder.query(&[("created_date", format!("lt.{}", before_date))]);
    }
    if let Some(after_date) = &params.after_date {
        req_builder = req_builder.query(&[("created_date", format!("gte.{}", after_date))]);
    }
    // TODO: implement metadata
    // if let Some(metadata) = params.metadata {
    //     req_builder = req_builder.query(&[("metadata", format!("eq.{}", metadata))]);
    // }

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

pub async fn datasets_post(
    configuration: &DatabaseApiConfig,
    // TODO: change this to just the metadata value and package that value into
    // the "metadata" key in the request body in this function.
    // Or send in a Dataset struct so serde can serialize that directly.
    request_body: serde_json::Value,
) -> Result<DatasetNoFiles> {
    debug!("building post request for: {:?}", request_body);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path("datasets");
    let mut req_builder = client.post(api_url.as_str());

    req_builder = req_builder.json(&request_body);

    let response = req_builder.send().await?;
    response.error_for_status_ref()?;

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("content: {}", content);

    // TODO: save json to file and prompt user to send it to us?
    let mut datasets: Vec<DatasetNoFiles> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
    // PostgREST resturns a list, even when only a single object is expected
    // https://postgrest.org/en/v7.0.0/api.html#singular-or-plural
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for newly-created Dataset!"))
}

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

pub async fn files_post(
    configuration: &DatabaseApiConfig,
    // TODO: change this to a Dataset struct
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
    // TODO: add context to 409 response (dataset doesn't exist) OR validate it does before uploading to storage provider

    debug!("status: {}", response.status());
    let content = response.text().await?;
    debug!("response content: {}", content);

    let mut uploaded_files: Vec<UploadedFile> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Files API was malformed: {}", &content))?;
    uploaded_files
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for updated File!"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use httpmock::{Method::GET, MockServer};

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
}
