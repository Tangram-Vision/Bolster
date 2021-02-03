// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use log::info;
use serde_json::json;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use uuid::Uuid;

use super::error;
use super::hazard;
use super::models;

use super::api;
use super::api::storage::StorageConfig;
use crate::app_config::AppConfig;

pub fn create_dataset() -> Result<()> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to storage provider

    let jwt = AppConfig::get::<String>("database.jwt")?;
    let config = api::Configuration::new(jwt);
    let dataset = api::datasets::datasets_post(
        &config,
        // TODO: create Dataset model to pass in or just json? metadata is only field needed
        json!({
            "metadata": {"description": "TODO: get from cmdline or prompt"},
            // TODO: remove url -- it will be moved to files table
            "url": "http://example.com",
        }),
    )?;
    // TODO: handle request error
    println!("{:?}", dataset);
    // TODO: display output (new dataset's uuid)
    Ok(())
}

pub fn list_datasets() -> Result<Vec<models::Dataset>> {
    let jwt = AppConfig::get::<String>("database.jwt")?;
    let config = api::Configuration::new(jwt);
    let datasets = api::datasets::datasets_get(
        &config, None, None, None, None, None, None, None, None, None, None, None,
    )?;

    // TODO: use generic, customizable formatter (e.g. kubernetes get)
    for d in datasets.iter() {
        println!("{} {}", d.uuid, d.created_date);
    }
    Ok(datasets)
}

// TODO: accept a callback for updating database entries?
// TODO: expect dataset uuid
pub fn upload_file(uuid: Uuid, path: &Path) -> Result<()> {
    // TODO: write a test for when file doesn't exist

    // TODO: change to
    // https://docs.rs/tokio/0.2.20/tokio/prelude/trait.AsyncRead.html or impl
    // of BufRead trait to handle big files
    let contents = fs::read(path)?;

    // TODO: prefix key with dataset uuid
    // TODO: test these error cases
    let key = path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid filename {:?}", path))?
        .to_str()
        .ok_or_else(|| anyhow!("Filename is invalid UTF8 {:?}", path))?;
    let key = format!("{}/{}", uuid, key);
    // Use DO bucket, region, and credentials if credentials are configured
    // Otherwise, try to us AWS S3 bucket/region/credentials
    // TODO: Refactor, something like? DoProvider::from(config).else(S3Provider::from(config)).else(Err)
    let config = AppConfig::fetch()?;
    let storage_config = StorageConfig::try_from(config)?;
    api::storage::upload_file(contents, key, storage_config)?;
    Ok(())
}

/// Show the configuration file
pub fn hazard() -> Result<()> {
    // Generate, randomly, True or False
    let random_hazard: bool = hazard::generate_hazard()?;

    if random_hazard {
        println!("You got it right!");
    } else {
        println!("You got it wrong!");
    }

    Ok(())
}

/// Show the configuration file
pub fn config() -> Result<()> {
    let config = AppConfig::fetch()?;
    println!("{:#?}", config);

    Ok(())
}

/// Simulate an error
pub fn simulate_error() -> Result<()> {
    // Log this Error simulation
    info!("We are simulating an error");

    // Simulate an error
    error::simulate_error()?;

    // We should never get here...
    Ok(())
}

#[cfg(test)]
mod test {
    use super::create_dataset;
    use crate::app_config::AppConfig;

    #[test]
    fn test_missing_database_jwt() {
        // Initialize configuration
        AppConfig::init(None).unwrap();
        let error = create_dataset().expect_err("Expected error due to missing database jwt");
        assert_eq!(
            error.to_string(),
            "configuration property \"database.jwt\" not found"
        );
    }
}
