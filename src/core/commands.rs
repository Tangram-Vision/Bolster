// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use log::debug;
use reqwest::Url;
use serde_json::json;
use std::convert::TryInto;
use std::fs;
use std::path::Path;
use uuid::Uuid;

use super::api::datasets::{self, DatabaseApiConfig, DatasetGetRequest};
use super::api::storage;
use super::api::storage::StorageConfig;
use super::models::{Dataset, UploadedFile};
use crate::app_config::{CompleteAppConfig, StorageProviderChoices};

pub fn create_dataset(config: &DatabaseApiConfig) -> Result<()> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to storage provider

    let dataset = datasets::datasets_post(
        config,
        // TODO: create Dataset model to pass in or just json?
        json!({
            "metadata": {"description": "TODO: get from cmdline or prompt"},
        }),
    )?;
    println!("Created new dataset with UUID: {}", dataset.dataset_id);
    Ok(())
}

pub fn list_datasets(
    config: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    let datasets = datasets::datasets_get(config, params)?;

    Ok(datasets)
}

/*
pub fn update_dataset(config: &DatabaseApiConfig, uuid: Uuid, url: &Url) -> Result<()> {
    // TODO: change to update files (not datasets) when files are their own db table

    datasets::datasets_patch(config, uuid, url)?;
    Ok(())
}
*/

pub fn add_file_to_dataset(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    url: &Url,
    filesize: i64,
    version: String,
    metadata: serde_json::Value,
) -> Result<()> {
    datasets::files_post(config, dataset_id, url, filesize, version, metadata)?;
    Ok(())
}

#[tokio::main]
pub async fn upload_file(
    config: StorageConfig,
    db_config: &DatabaseApiConfig,
    dataset_id: Uuid,
    path: &Path,
    prefix: &str,
) -> Result<()> {
    let filesize: i64 = fs::metadata(path)?.len().try_into().unwrap();
    // TODO: test what a good threshold is (or expose it as CLI option)
    const MULTIPART_FILESIZE_THRESHOLD: i64 = 20 * 1024 * 1024;
    let key = path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid filename {:?}", path))?
        .to_str()
        .ok_or_else(|| anyhow!("Filename is invalid UTF8 {:?}", path))?;
    let key = format!("{}/{}/{}", prefix, dataset_id, key);
    let metadata = json!({});

    if filesize < MULTIPART_FILESIZE_THRESHOLD {
        debug!(
            "Filesize {} < threshold {} so doing oneshot",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) = storage::upload_file_oneshot(config, path, filesize, key).await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata)?;
    } else {
        debug!(
            "Filesize {} > threshold {} so doing multipart",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) = storage::upload_file_multipart(config, path, filesize, key).await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata)?;
    }

    // TODO: add progress bar for upload/download
    // TODO: spawn upload/download task with channel back to "main" task, which receives progress updates (that it can print to stdout) until upload/download task ends

    // TODO: trigger calibration pipeline after all files have uploaded successfully

    Ok(())
}

pub fn list_files(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    filename: &str,
) -> Result<Vec<UploadedFile>> {
    let files = datasets::files_get(config, dataset_id, filename)?;

    Ok(files)
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
mod tests {
    use super::*;
    use crate::app_config::{DatabaseConfig, StorageProviderChoices};
    use crate::core::api::datasets::DatabaseApiConfig;

    #[test]
    fn test_upload_missing_file() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                include_str!("../resources/test_full_config.toml"),
                config::FileFormat::Toml,
            ))
            .unwrap();

        let db = config
            .clone()
            .try_into::<DatabaseConfig>()
            .unwrap()
            .database;
        let db_config = DatabaseApiConfig::new(db.url.clone(), db.jwt.clone()).unwrap();
        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let dataset_id = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new("nonexistent-file");
        let prefix = "";
        let error = upload_file(storage_config, &db_config, dataset_id, path, prefix)
            .expect_err("Loading nonexistent file should fail");
        assert!(
            error.to_string().contains("No such file or directory"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn test_upload_invalid_filename() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                include_str!("../resources/test_full_config.toml"),
                config::FileFormat::Toml,
            ))
            .unwrap();

        let db = config
            .clone()
            .try_into::<DatabaseConfig>()
            .unwrap()
            .database;
        let db_config = DatabaseApiConfig::new(db.url.clone(), db.jwt.clone()).unwrap();
        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let dataset_id = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new("/");
        let prefix = "";
        let error = upload_file(storage_config, &db_config, dataset_id, path, prefix)
            .expect_err("Loading bad filename should fail");
        assert!(
            error.to_string().contains("Invalid filename"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn test_printing_bogus_config() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                "[bad_header]\nbad_key = \"bad value\"",
                config::FileFormat::Toml,
            ))
            .unwrap();

        let error = print_config(config).expect_err("Unexpected config format should error");
        assert!(
            error.to_string().contains("missing field"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn test_bad_storage_config() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                "[blah]\naccess_key = \"whatever\"",
                config::FileFormat::Toml,
            ))
            .unwrap();

        let url_str = "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/test";
        let url = Url::parse(&url_str).unwrap();
        let error = download_file(config, &url).expect_err("Missing storage config should error");
        assert!(
            error.to_string().contains("missing field"),
            "{}",
            error.to_string()
        );
    }
}
