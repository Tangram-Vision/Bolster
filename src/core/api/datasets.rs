// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use reqwest;

use super::{configuration, Error};
use crate::apis::ResponseContent;

/// struct for typed errors of method `datasets_delete`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetsDeleteError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method `datasets_get`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetsGetError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method `datasets_patch`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetsPatchError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method `datasets_post`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetsPostError {
    UnknownValue(serde_json::Value),
}

pub async fn datasets_delete(
    configuration: &configuration::Configuration,
    uuid: Option<&str>,
    created_date: Option<&str>,
    creator_role: Option<&str>,
    access_role: Option<&str>,
    url: Option<&str>,
    metadata: Option<&str>,
    prefer: Option<&str>,
) -> Result<(), Error<DatasetsDeleteError>> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.delete(local_var_uri_str.as_str());

    if let Some(ref local_var_str) = uuid {
        local_var_req_builder =
            local_var_req_builder.query(&[("uuid", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = created_date {
        local_var_req_builder =
            local_var_req_builder.query(&[("created_date", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = creator_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("creator_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = access_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("access_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = url {
        local_var_req_builder = local_var_req_builder.query(&[("url", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = metadata {
        local_var_req_builder =
            local_var_req_builder.query(&[("metadata", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_user_agent) = configuration.user_agent {
        local_var_req_builder =
            local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(local_var_param_value) = prefer {
        local_var_req_builder =
            local_var_req_builder.header("Prefer", local_var_param_value.to_string());
    }

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if local_var_status.is_success() {
        Ok(())
    } else {
        let local_var_entity: Option<DatasetsDeleteError> =
            serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn datasets_get(
    configuration: &configuration::Configuration,
    uuid: Option<&str>,
    created_date: Option<&str>,
    creator_role: Option<&str>,
    access_role: Option<&str>,
    url: Option<&str>,
    metadata: Option<&str>,
    select: Option<&str>,
    order: Option<&str>,
    range: Option<&str>,
    range_unit: Option<&str>,
    offset: Option<&str>,
    limit: Option<&str>,
    prefer: Option<&str>,
) -> Result<Vec<crate::models::Datasets>, Error<DatasetsGetError>> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.get(local_var_uri_str.as_str());

    if let Some(ref local_var_str) = uuid {
        local_var_req_builder =
            local_var_req_builder.query(&[("uuid", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = created_date {
        local_var_req_builder =
            local_var_req_builder.query(&[("created_date", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = creator_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("creator_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = access_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("access_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = url {
        local_var_req_builder = local_var_req_builder.query(&[("url", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = metadata {
        local_var_req_builder =
            local_var_req_builder.query(&[("metadata", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = select {
        local_var_req_builder =
            local_var_req_builder.query(&[("select", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = order {
        local_var_req_builder =
            local_var_req_builder.query(&[("order", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = offset {
        local_var_req_builder =
            local_var_req_builder.query(&[("offset", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = limit {
        local_var_req_builder =
            local_var_req_builder.query(&[("limit", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_user_agent) = configuration.user_agent {
        local_var_req_builder =
            local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(local_var_param_value) = range {
        local_var_req_builder =
            local_var_req_builder.header("Range", local_var_param_value.to_string());
    }
    if let Some(local_var_param_value) = range_unit {
        local_var_req_builder =
            local_var_req_builder.header("Range-Unit", local_var_param_value.to_string());
    }
    if let Some(local_var_param_value) = prefer {
        local_var_req_builder =
            local_var_req_builder.header("Prefer", local_var_param_value.to_string());
    }

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if local_var_status.is_success() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<DatasetsGetError> =
            serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn datasets_patch(
    configuration: &configuration::Configuration,
    uuid: Option<&str>,
    created_date: Option<&str>,
    creator_role: Option<&str>,
    access_role: Option<&str>,
    url: Option<&str>,
    metadata: Option<&str>,
    prefer: Option<&str>,
    datasets: Option<crate::models::Datasets>,
) -> Result<(), Error<DatasetsPatchError>> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.patch(local_var_uri_str.as_str());

    if let Some(ref local_var_str) = uuid {
        local_var_req_builder =
            local_var_req_builder.query(&[("uuid", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = created_date {
        local_var_req_builder =
            local_var_req_builder.query(&[("created_date", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = creator_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("creator_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = access_role {
        local_var_req_builder =
            local_var_req_builder.query(&[("access_role", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = url {
        local_var_req_builder = local_var_req_builder.query(&[("url", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = metadata {
        local_var_req_builder =
            local_var_req_builder.query(&[("metadata", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_user_agent) = configuration.user_agent {
        local_var_req_builder =
            local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(local_var_param_value) = prefer {
        local_var_req_builder =
            local_var_req_builder.header("Prefer", local_var_param_value.to_string());
    }
    local_var_req_builder = local_var_req_builder.json(&datasets);

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if local_var_status.is_success() {
        Ok(())
    } else {
        let local_var_entity: Option<DatasetsPatchError> =
            serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn datasets_post(
    configuration: &configuration::Configuration,
    select: Option<&str>,
    prefer: Option<&str>,
    datasets: Option<crate::models::Datasets>,
) -> Result<(), Error<DatasetsPostError>> {
    let local_var_client = &configuration.client;

    let local_var_uri_str = format!("{}/datasets", configuration.base_path);
    let mut local_var_req_builder = local_var_client.post(local_var_uri_str.as_str());

    if let Some(ref local_var_str) = select {
        local_var_req_builder =
            local_var_req_builder.query(&[("select", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_user_agent) = configuration.user_agent {
        local_var_req_builder =
            local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(local_var_param_value) = prefer {
        local_var_req_builder =
            local_var_req_builder.header("Prefer", local_var_param_value.to_string());
    }
    local_var_req_builder = local_var_req_builder.json(&datasets);

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if local_var_status.is_success() {
        Ok(())
    } else {
        let local_var_entity: Option<DatasetsPostError> =
            serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };
        Err(Error::ResponseError(local_var_error))
    }
}
