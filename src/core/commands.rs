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

use super::api::datasets::{self, DatabaseApiConfig, DatasetGetRequest};
use super::api::storage;
use super::api::storage::StorageConfig;
use super::models::Dataset;
use crate::app_config::{CompleteAppConfig, StorageProviderChoices};

pub fn create_dataset(config: &DatabaseApiConfig) -> Result<()> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to storage provider

    let dataset = datasets::datasets_post(
        config,
        // TODO: create Dataset model to pass in or just json?
        json!({
            "metadata": {"description": "TODO: get from cmdline or prompt"},
            // TODO: remove url -- it will be moved to files table
            "url": "http://example.com",
        }),
    )?;
    println!("Created new dataset with UUID: {}", dataset.uuid);
    Ok(())
}

pub fn list_datasets(
    config: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    let datasets = datasets::datasets_get(config, params)?;

    Ok(datasets)
}

pub fn update_dataset(config: &DatabaseApiConfig, uuid: Uuid, url: &Url) -> Result<()> {
    // TODO: change to update files (not datasets) when files are their own db table

    datasets::datasets_patch(config, uuid, url)?;
    Ok(())
}

pub fn upload_file(config: StorageConfig, uuid: Uuid, path: &Path) -> Result<Url> {
    let key = path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid filename {:?}", path))?
        .to_str()
        .ok_or_else(|| anyhow!("Filename is invalid UTF8 {:?}", path))?;
    let key = format!("{}/{}", uuid, key);

    // TODO: change to
    // https://docs.rs/tokio/0.2.20/tokio/prelude/trait.AsyncRead.html or impl
    // of BufRead trait to handle big files
    let contents = fs::read(path)?;

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
mod tests {
    use super::*;
    use std::ffi;
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn test_upload_missing_file() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                include_str!("../resources/test_full_config.toml"),
                config::FileFormat::Toml,
            ))
            .unwrap();

        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let uuid = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new("nonexistent-file");
        let error = upload_file(storage_config, uuid, path)
            .expect_err("Loading nonexistent file should fail");
        assert!(
            error.to_string().contains("No such file or directory"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn test_upload_invalid_filename_utf8() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                include_str!("../resources/test_full_config.toml"),
                config::FileFormat::Toml,
            ))
            .unwrap();

        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let uuid = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new(ffi::OsStr::from_bytes(&[128u8]));
        let error =
            upload_file(storage_config, uuid, path).expect_err("Loading bad filename should fail");
        assert!(
            error.to_string().contains("Filename is invalid UTF8"),
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

        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let uuid = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new("/");
        let error =
            upload_file(storage_config, uuid, path).expect_err("Loading bad filename should fail");
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

        let url_str = "https://tangram-datasets.s3.us-east-2.amazonaws.com/test";
        let url = Url::parse(&url_str).unwrap();
        let error = download_file(config, &url).expect_err("Missing storage config should error");
        assert!(
            error.to_string().contains("missing field"),
            "{}",
            error.to_string()
        );
    }
}
