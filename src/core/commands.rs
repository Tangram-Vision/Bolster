// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use reqwest::Url;
use serde_json::json;
use std::fs;
use std::path::Path;
use uuid::Uuid;

use super::api;
use super::api::datasets;
use super::api::storage;
use super::api::storage::StorageConfig;
use super::models::Dataset;
use crate::app_config::{CompleteAppConfig, StorageProviderChoices};

pub fn create_dataset(config: &api::Configuration) -> Result<()> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to storage provider

    // TODO: derive api::Configuration from config::Config
    // TODO: do it in cli.rs before calling any subcommands? printing out config doesn't require api config though
    let dataset = datasets::datasets_post(
        config,
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

pub fn list_datasets(config: &api::Configuration, uuid: Option<Uuid>) -> Result<Vec<Dataset>> {
    let datasets = datasets::datasets_get(
        config, uuid, None, None, None, None, None, None, None, None, None, None,
    )?;

    Ok(datasets)
}

pub fn update_dataset(config: &api::Configuration, uuid: Uuid, url: &Url) -> Result<()> {
    // TODO: change to update files (not datasets) when files are their own db table

    let dataset = datasets::datasets_patch(config, uuid, url)?;
    // TODO: handle request error
    println!("{:?}", dataset);
    // TODO: display output (new dataset's uuid)
    Ok(())
}

pub fn upload_file(config: StorageConfig, uuid: Uuid, path: &Path) -> Result<Url> {
    // TODO: write a test for when file doesn't exist

    // TODO: change to
    // https://docs.rs/tokio/0.2.20/tokio/prelude/trait.AsyncRead.html or impl
    // of BufRead trait to handle big files
    let contents = fs::read(path)?;

    // TODO: test these error cases
    let key = path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid filename {:?}", path))?
        .to_str()
        .ok_or_else(|| anyhow!("Filename is invalid UTF8 {:?}", path))?;
    let key = format!("{}/{}", uuid, key);

    let url = storage::upload_file(config, contents, key)?;
    Ok(url)
}

pub fn download_file(config: config::Config, url: &Url) -> Result<()> {
    // Based on url from database, find which StorageProvider's config to use
    let provider = StorageProviderChoices::from_url(url)?;
    let storage_config = StorageConfig::new(config, provider)?;

    storage::download_file(storage_config, url)?;
    Ok(())
}

/// Show the configuration file
pub fn print_config(config: config::Config) -> Result<()> {
    let storage_config: CompleteAppConfig = config.try_into()?;
    println!("{}", toml::to_string(&storage_config)?);

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::app_config::DatabaseConfig;

    #[test]
    fn test_missing_database_jwt() {
        // Initialize configuration
        let config = config::Config::default();
        let error = config
            .try_into::<DatabaseConfig>()
            .expect_err("Expected error due to missing database jwt");
        assert_eq!(
            error.to_string(),
            "configuration property \"database.jwt\" not found"
        );
    }
}
