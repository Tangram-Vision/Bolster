// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use log::debug;
use reqwest::{header, Url};
use serde_json::json;
use std::time::Duration;
use strum_macros::{Display, EnumString, EnumVariantNames};
use uuid::Uuid;

use crate::core::models::Dataset;

pub struct DatabaseApiConfig {
    pub base_url: String,
    pub client: reqwest::blocking::Client,
}

impl DatabaseApiConfig {
    pub fn new_with_params(
        bearer_access_token: String,
        base_url: String,
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
            client: reqwest::blocking::Client::builder()
                .user_agent(user_agent)
                .default_headers(headers)
                .timeout(Duration::from_secs(timeout))
                .build()?,
            base_url,
        })
    }

    pub fn new(bearer_access_token: String) -> Result<Self> {
        let base_url = "http://0.0.0.0:3000".to_owned();
        let timeout = 30;
        Self::new_with_params(bearer_access_token, base_url, timeout)
    }
}

// Only allow a single sort key for now
#[derive(EnumString, EnumVariantNames, Display, Debug)]
pub enum DatasetOrdering {
    #[strum(serialize = "created_date.asc")]
    CreatedDateAsc,
    #[strum(serialize = "created_date.desc")]
    CreatedDateDesc,
    #[strum(serialize = "creator.asc")]
    CreatorAsc,
    #[strum(serialize = "creator.desc")]
    CreatorDesc,
}

impl DatasetOrdering {
    // For possible dataset ordering options where the CLI name (e.g. "creator")
    // doesn't match the API/database name (e.g. "creator_role"), translate
    // between them
    fn to_database_field(&self) -> String {
        match self {
            DatasetOrdering::CreatorAsc => "creator_role.asc".to_owned(),
            DatasetOrdering::CreatorDesc => "creator_role.desc".to_owned(),
            other => other.to_string(),
            // TODO: test order by creator
        }
    }
}

#[derive(Debug)]
pub struct DatasetGetRequest {
    pub uuid: Option<Uuid>,
    pub before_date: Option<NaiveDate>,
    pub after_date: Option<NaiveDate>,
    pub creator: Option<String>,
    // TODO: implement metadata: Option<String>,
    pub order: Option<DatasetOrdering>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Default for DatasetGetRequest {
    fn default() -> Self {
        Self {
            uuid: None,
            before_date: None,
            after_date: None,
            creator: None,
            order: None,
            limit: None,
            offset: None,
        }
    }
}

pub fn datasets_patch(
    configuration: &DatabaseApiConfig,
    uuid: Uuid,
    new_url: &Url,
) -> Result<Dataset> {
    debug!("building patch request for: {}", uuid);
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_url);
    let mut req_builder = client.patch(url.as_str());

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

pub fn datasets_get(
    configuration: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    debug!("building get request for: {:?}", params);
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_url);
    let mut req_builder = client.get(url.as_str());

    if let Some(uuid) = &params.uuid {
        req_builder = req_builder.query(&[("uuid", format!("eq.{}", uuid))]);
    }
    if let Some(before_date) = &params.before_date {
        req_builder = req_builder.query(&[("created_date", format!("lt.{}", before_date))]);
    }
    if let Some(after_date) = &params.after_date {
        req_builder = req_builder.query(&[("created_date", format!("gte.{}", after_date))]);
    }
    if let Some(creator) = &params.creator {
        req_builder = req_builder.query(&[("creator_role", format!("eq.{}", creator))]);
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

    let request = req_builder.build()?;
    let response = client.execute(request)?.error_for_status()?;

    debug!("status: {}", response.status());
    let content = response.text()?;
    debug!("content: {}", content);

    let datasets: Vec<Dataset> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
    Ok(datasets)
}

pub fn datasets_post(
    configuration: &DatabaseApiConfig,
    request_body: serde_json::Value,
) -> Result<Dataset> {
    debug!("building post request for: {:?}", request_body);
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_url);
    let mut req_builder = client.post(url.as_str());

    req_builder = req_builder.json(&request_body);

    let request = req_builder.build()?;
    let response = client.execute(request)?.error_for_status()?;

    debug!("status: {}", response.status());
    let content = response.text()?;
    debug!("content: {}", content);

    // TODO: save json to file and prompt user to send it to us?
    let mut datasets: Vec<Dataset> = serde_json::from_str(&content)
        .with_context(|| format!("JSON from Datasets API was malformed: {}", &content))?;
    // PostgREST resturns a list, even when only a single object is expected
    // https://postgrest.org/en/v7.0.0/api.html#singular-or-plural
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for newly-created Dataset!"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use std::str::FromStr;

    #[test]
    fn test_datasets_get_success() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{"uuid": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584",
                    "creator_role": "tangram_user",
                    "access_role": "tangram_user",
                    "url": "https://example.com/afd56ecf-9d87-4053-8c80-0d924f06da52/hello.txt",
                    "metadata": {
                        "description": "Test"
                    }
                }]));
        });

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 10)
                .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).unwrap();

        mock.assert();
        assert_eq!(result[0].uuid, "afd56ecf-9d87-4053-8c80-0d924f06da52");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_datasets_get_query_params() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .query_param("created_date", "gte.2021-01-01")
                .query_param("order", "creator_role.desc")
                .query_param("limit", "17")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{"uuid": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584",
                    "creator_role": "tangram_user",
                    "access_role": "tangram_user",
                    "url": "https://example.com/afd56ecf-9d87-4053-8c80-0d924f06da52/hello.txt",
                    "metadata": {
                        "description": "Test"
                    }
                }]));
        });

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 10)
                .unwrap();
        let params = DatasetGetRequest {
            after_date: Some(NaiveDate::from_str("2021-01-01").unwrap()),
            order: Some(DatasetOrdering::CreatorDesc),
            limit: Some(17),
            ..Default::default()
        };

        let result = datasets_get(&config, &params).unwrap();

        mock.assert();
        assert_eq!(result[0].uuid, "afd56ecf-9d87-4053-8c80-0d924f06da52");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_datasets_get_wrong_structure_json() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!({"uuid": "afd56ecf-9d87-4053-8c80-0d924f06da52",
                    "created_date": "2021-02-03T21:21:57.713584",
                    "creator_role": "tangram_user",
                    "access_role": "tangram_user",
                    "url": "https://example.com/afd56ecf-9d87-4053-8c80-0d924f06da52/hello.txt",
                    "metadata": {
                        "description": "Test"
                    }
                }));
        });

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 10)
                .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).expect_err("Expected json parsing error");
        let downcast = result.downcast_ref::<serde_json::Error>().unwrap();

        mock.assert();
        // Expected json structure is a list of maps, not a single map
        assert_eq!(downcast.classify(), serde_json::error::Category::Data);
        assert!(result
            .to_string()
            .contains("JSON from Datasets API was malformed: {"));
    }

    #[test]
    fn test_datasets_get_malformed_json() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .body("this isn't actually json");
        });

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 10)
                .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).expect_err("Expected json parsing error");
        let downcast = result.downcast_ref::<serde_json::Error>().unwrap();

        mock.assert();
        assert_eq!(downcast.classify(), serde_json::error::Category::Syntax);
        assert!(result
            .to_string()
            .contains("JSON from Datasets API was malformed: this isn't actually json"));
    }

    #[test]
    fn test_datasets_get_401_response() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer TEST-TOKEN")
                .path("/datasets");
            then.status(401)
                .header("Content-Type", "application/json")
                .json_body(json!({"message": "JWSError JWSInvalidSignature"}));
        });

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 10)
                .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).expect_err("Expected status code error");
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

    #[test]
    fn test_datasets_get_timeout() {
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

        let config =
            DatabaseApiConfig::new_with_params("TEST-TOKEN".to_owned(), server.base_url(), 1)
                .unwrap();
        let params = DatasetGetRequest::default();

        let result = datasets_get(&config, &params).expect_err("Expected timeout error");
        let downcast = result.downcast_ref::<reqwest::Error>().unwrap();

        mock.assert();
        assert!(downcast.is_timeout());
        assert!(result.to_string().contains("operation timed out"));
    }
}
