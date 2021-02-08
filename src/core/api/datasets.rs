// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use crate::core::models::Dataset;
use anyhow::{anyhow, Result};
use reqwest::Url;
use serde_json::json;
use uuid::Uuid;

pub fn datasets_patch(
    configuration: &super::Configuration,
    uuid: Uuid,
    url: &Url,
) -> Result<Dataset> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.patch(local_var_uri_str.as_str());

    local_var_req_builder =
        local_var_req_builder.query(&[("uuid", format!("eq.{}", uuid.to_string()))]);

    local_var_req_builder = local_var_req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    local_var_req_builder = local_var_req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of updated Dataset in response
    local_var_req_builder = local_var_req_builder.header("Prefer", "return=representation");

    let req_body = json!({ "url": url });
    local_var_req_builder = local_var_req_builder.json(&req_body);

    let local_var_req = local_var_req_builder.build()?;
    println!("request: {:?}", local_var_req);
    let local_var_resp = local_var_client.execute(local_var_req)?;

    println!("status: {}", local_var_resp.status());
    let local_var_content = local_var_resp.text()?;
    println!("response content: {}", local_var_content);

    let mut datasets: Vec<Dataset> = serde_json::from_str(&local_var_content)?;
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for updated Dataset!"))
}

pub fn datasets_get(
    configuration: &super::Configuration,
    uuid: Option<Uuid>,
    created_date: Option<&str>,
    creator_role: Option<&str>,
    access_role: Option<&str>,
    url: Option<&Url>,
    metadata: Option<&str>,
    order: Option<&str>,
    range: Option<&str>,
    range_unit: Option<&str>,
    offset: Option<&str>,
    limit: Option<&str>,
) -> Result<Vec<Dataset>> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.get(local_var_uri_str.as_str());

    if let Some(ref local_var_str) = uuid {
        local_var_req_builder =
            local_var_req_builder.query(&[("uuid", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = created_date {
        local_var_req_builder = local_var_req_builder
            .query(&[("created_date", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = creator_role {
        local_var_req_builder = local_var_req_builder
            .query(&[("creator_role", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = access_role {
        local_var_req_builder = local_var_req_builder
            .query(&[("access_role", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = url {
        local_var_req_builder =
            local_var_req_builder.query(&[("url", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = metadata {
        local_var_req_builder = local_var_req_builder
            .query(&[("metadata", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = order {
        local_var_req_builder =
            local_var_req_builder.query(&[("order", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = offset {
        local_var_req_builder = local_var_req_builder
            .query(&[("offset", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(ref local_var_str) = limit {
        local_var_req_builder =
            local_var_req_builder.query(&[("limit", format!("eq.{}", &local_var_str.to_string()))]);
    }
    if let Some(local_var_param_value) = range {
        local_var_req_builder =
            local_var_req_builder.header("Range", local_var_param_value.to_string());
    }
    if let Some(local_var_param_value) = range_unit {
        local_var_req_builder =
            local_var_req_builder.header("Range-Unit", local_var_param_value.to_string());
    }

    local_var_req_builder = local_var_req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    local_var_req_builder = local_var_req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of created Dataset in response
    local_var_req_builder = local_var_req_builder.header("Prefer", "return=representation");

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client
        .execute(local_var_req)?
        .error_for_status()?;

    println!("status: {}", local_var_resp.status());
    let local_var_content = local_var_resp.text()?;
    println!("content: {}", local_var_content);

    let datasets: Vec<Dataset> = serde_json::from_str(&local_var_content)?;
    Ok(datasets)
}

pub fn datasets_post(
    configuration: &super::Configuration,
    // Select is to pick specific fields to return in response
    // select: Option<&str>,
    request_body: serde_json::Value,
) -> Result<Dataset> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.post(local_var_uri_str.as_str());

    /*
    if let Some(ref local_var_str) = select {
        local_var_req_builder =
            local_var_req_builder.query(&[("select", &local_var_str.to_string())]);
    }
    */
    local_var_req_builder = local_var_req_builder.header(
        reqwest::header::USER_AGENT,
        configuration.user_agent.clone(),
    );
    // Use JWT for auth
    local_var_req_builder = local_var_req_builder.header(
        "Authorization",
        format!("Bearer {}", configuration.bearer_access_token),
    );
    // Get json of created Dataset in response
    local_var_req_builder = local_var_req_builder.header("Prefer", "return=representation");

    println!("reqbody: {}", request_body);
    local_var_req_builder = local_var_req_builder.json(&request_body);

    let local_var_req = local_var_req_builder.build()?;
    println!("headers: {:?}", local_var_req.headers());
    let local_var_resp = local_var_client
        .execute(local_var_req)?
        .error_for_status()?;

    println!("status: {}", local_var_resp.status());
    let local_var_content = local_var_resp.text()?;
    println!("content: {}", local_var_content);

    let mut datasets: Vec<Dataset> = serde_json::from_str(&local_var_content)?;
    datasets
        .pop()
        .ok_or_else(|| anyhow!("Database returned no info for newly-created Dataset!"))
}

#[cfg(test)]
mod test {
    // TODO: how to mock responses? test network and server failures, 502 responses, etc.
}
