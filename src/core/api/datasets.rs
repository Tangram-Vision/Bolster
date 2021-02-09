// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use reqwest::Url;
use serde_json::json;
use uuid::Uuid;

use crate::core::models::Dataset;

pub fn datasets_patch(
    configuration: &super::Configuration,
    uuid: Uuid,
    new_url: &Url,
) -> Result<Dataset> {
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_path);
    let mut req_builder = client.patch(url.as_str());

    req_builder = req_builder.query(&[("uuid", format!("eq.{}", uuid.to_string()))]);

    req_builder = req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    req_builder = req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of updated Dataset in response
    req_builder = req_builder.header("Prefer", "return=representation");

    let req_body = json!({ "url": new_url });
    req_builder = req_builder.json(&req_body);

    let request = req_builder.build()?;
    println!("request: {:?}", request);
    let response = client.execute(request)?;

    println!("status: {}", response.status());
    let content = response.text()?;
    println!("response content: {}", content);

    // Move deserialization to response.json call?
    let mut datasets: Vec<Dataset> = serde_json::from_str(&content)?;
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for updated Dataset!"))
}

pub struct DatasetGetRequest {
    pub uuid: Option<Uuid>,
    pub before_date: Option<NaiveDate>,
    pub after_date: Option<NaiveDate>,
    pub creator: Option<String>,
    // TODO: implement metadata: Option<String>,
    // TODO: enum of Dataset cols?
    pub order: Option<String>,
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

pub fn datasets_get(
    configuration: &super::Configuration,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_path);
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

    // TODO: fix how order is added to query string
    if let Some(order) = &params.order {
        req_builder = req_builder.query(&[("order", format!("eq.{}", order))]);
    }
    if let Some(limit) = &params.limit {
        req_builder = req_builder.query(&[("limit", limit)]);
    }
    if let Some(offset) = &params.offset {
        req_builder = req_builder.query(&[("offset", offset)]);
    }

    req_builder = req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    req_builder = req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of created Dataset in response
    req_builder = req_builder.header("Prefer", "return=representation");

    let request = req_builder.build()?;
    let response = client.execute(request)?.error_for_status()?;

    println!("status: {}", response.status());
    let content = response.text()?;
    println!("content: {}", content);

    let datasets: Vec<Dataset> = serde_json::from_str(&content)?;
    Ok(datasets)
}

pub fn datasets_post(
    configuration: &super::Configuration,
    request_body: serde_json::Value,
) -> Result<Dataset> {
    let client = &configuration.client;

    let url = format!("{}/datasets", configuration.base_path);
    let mut req_builder = client.post(url.as_str());

    req_builder = req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    req_builder = req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of created Dataset in response
    req_builder = req_builder.header("Prefer", "return=representation");

    println!("reqbody: {}", request_body);
    req_builder = req_builder.json(&request_body);

    let request = req_builder.build()?;
    println!("headers: {:?}", request.headers());
    let response = client.execute(request)?.error_for_status()?;

    println!("status: {}", response.status());
    let content = response.text()?;
    println!("content: {}", content);

    let mut datasets: Vec<Dataset> = serde_json::from_str(&content)?;
    // PostgREST resturns a list, even when only a single object is expected
    // https://postgrest.org/en/v7.0.0/api.html#singular-or-plural
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for newly-created Dataset!"))
}

#[cfg(test)]
mod test {
    // TODO: how to mock responses? test network and server failures, 502 responses, etc.
}
