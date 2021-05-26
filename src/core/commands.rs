// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use futures::stream;
use futures::stream::StreamExt;
use log::debug;
use reqwest::Url;
use serde_json::json;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::api::datasets::{self, DatabaseApiConfig, DatasetGetRequest};
use super::api::storage;
use super::api::storage::StorageConfig;
use super::models::{Dataset, UploadedFile};
use crate::app_config::{CompleteAppConfig, StorageProviderChoices};

pub async fn create_dataset(config: &DatabaseApiConfig) -> Result<Uuid> {
    // TODO: create Dataset model to pass in or just json?
    let dataset = datasets::datasets_post(config, json!({})).await?;
    Ok(dataset.dataset_id)
}

pub async fn create_and_upload_dataset(
    config: StorageConfig,
    db_config: &DatabaseApiConfig,
    prefix: &str,
    file_paths: Vec<&Path>,
) -> Result<()> {
    // TODO: create dataset and all files (w/ not-uploaded state) in single API call?

    let dataset_id: Uuid = create_dataset(db_config).await?;

    println!("Created new dataset with UUID: {}", dataset_id);
    debug!("paths: {:?}", file_paths);

    // TODO: Make this configurable?
    const MAX_FILES_UPLOADING_CONCURRENTLY: usize = 4;

    // Uploads to storage AND registers to database
    let mut futs = stream::iter(
        file_paths
            .iter()
            .map(|path| upload_file(config.clone(), db_config, dataset_id, path, prefix)),
    )
    .buffer_unordered(MAX_FILES_UPLOADING_CONCURRENTLY);
    while let Some(res) = futs.next().await {
        res?;
    }

    Ok(())
}

pub async fn list_datasets(
    config: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    let datasets = datasets::datasets_get(config, params).await?;

    Ok(datasets)
}

/*
pub async fn update_dataset(config: &DatabaseApiConfig, uuid: Uuid, url: &Url) -> Result<()> {
    // TODO: change to update files (not datasets) when files are their own db table

    datasets::datasets_patch(config, uuid, url)?;
    Ok(())
}
*/

pub async fn add_file_to_dataset(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    url: &Url,
    filesize: i64,
    version: String,
    metadata: serde_json::Value,
) -> Result<()> {
    datasets::files_post(config, dataset_id, url, filesize, version, metadata).await?;
    Ok(())
}

pub async fn upload_file(
    config: StorageConfig,
    db_config: &DatabaseApiConfig,
    dataset_id: Uuid,
    path: &Path,
    prefix: &str,
) -> Result<()> {
    // This threshold determines when we switch from one-shot upload (using
    // PutObject API) to a multipart upload (using CreateMultipartUpload,
    // UploadPart, and CompleteMultipartUpload APIs).
    //
    // The threshold is set to 64MB (currently yielding 4x 16MB part uploads).
    // The threshold is set a bit arbitrarily -- it is above 64MB that the
    // multipart upload starts being faster than one-shot uploads. Below 64MB,
    // the extra overhead of extra API calls makes multipart uploads slower.
    const MULTIPART_FILESIZE_THRESHOLD: i64 = 64 * 1024 * 1024;

    // We retain any directories in the path
    let key = path
        .to_str()
        .ok_or_else(|| anyhow!("Invalid filename (must be UTF8) {:?}", path))?;
    let key = format!("{}/{}/{}", prefix, dataset_id, key);
    debug!("key {}", key);

    debug!("Got path {:?}", path);
    let filesize: i64 = tokio::fs::metadata(path).await?.len().try_into().unwrap();

    let metadata = json!({});

    if filesize < MULTIPART_FILESIZE_THRESHOLD {
        debug!(
            "Filesize {} < threshold {} so doing oneshot",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) = storage::upload_file_oneshot(config, path, filesize, key).await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata).await?;
    } else {
        debug!(
            "Filesize {} > threshold {} so doing multipart",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) =
            storage::upload_file_multipart(config, path, filesize as usize, key).await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata).await?;
    }

    // TODO: add progress bar for upload/download
    // TODO: spawn upload/download task with channel back to "main" task, which receives progress updates (that it can print to stdout) until upload/download task ends

    // TODO: trigger calibration pipeline after all files have uploaded successfully

    Ok(())
}

pub async fn list_files(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    prefixes: Vec<String>,
) -> Result<Vec<UploadedFile>> {
    datasets::files_get(config, dataset_id, prefixes).await
}

pub async fn download_files(config: config::Config, file_paths: Vec<UploadedFile>) -> Result<()> {
    // TODO: Make this configurable?
    const MAX_FILES_DOWNLOADING_CONCURRENTLY: usize = 4;

    if file_paths.is_empty() {
        Ok(())
    } else {
        // Based on url from database, find which StorageProvider's config to use
        let provider = StorageProviderChoices::from_url(&file_paths[0].url)?;
        let storage_config = StorageConfig::new(config, provider)?;

        let mut futs = stream::iter(
            file_paths
                .iter()
                .map(|file| download_file_and_provide_filepath(storage_config.clone(), file)),
        )
        .buffer_unordered(MAX_FILES_DOWNLOADING_CONCURRENTLY);
        while let Some(res) = futs.next().await {
            let (bytestream, filepath) = res?;
            let mut async_data = bytestream.into_async_read();
            if let Some(dir) = filepath.parent() {
                tokio::fs::create_dir_all(dir).await?;
            }
            let mut file = tokio::fs::File::create(filepath).await?;
            tokio::io::copy(&mut async_data, &mut file).await?;
        }

        Ok(())
    }
}

// Helper function to provide filepath along with downloaded file data
async fn download_file_and_provide_filepath(
    config: storage::StorageConfig,
    file: &UploadedFile,
) -> Result<(rusoto_core::ByteStream, PathBuf)> {
    let async_data = storage::download_file(config.clone(), &file.url).await?;
    let filepath = file.filepath_from_url()?;
    Ok((async_data, filepath))
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
    use chrono::Utc;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_upload_missing_file() {
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
        let db_config = DatabaseApiConfig::new(db.url.clone(), db.jwt).unwrap();
        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let dataset_id = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        let path = Path::new("nonexistent-file");
        let prefix = "";
        let error = upload_file(storage_config, &db_config, dataset_id, path, prefix)
            .await
            .expect_err("Loading nonexistent file should fail");
        assert!(
            error.to_string().contains("No such file or directory"),
            "{}",
            error.to_string()
        );
    }

    #[tokio::test]
    async fn test_upload_invalid_filename() {
        let _ = env_logger::try_init();

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
        let db_config = DatabaseApiConfig::new(db.url.clone(), db.jwt).unwrap();
        let storage_config = StorageConfig::new(config, StorageProviderChoices::Aws).unwrap();
        let dataset_id = Uuid::parse_str("619e0899-ec94-4d87-812c-71736c09c4d6").unwrap();
        // https://users.rust-lang.org/t/i-need-a-few-examples-of-non-utf8-osstring-pathbuf-for-my-unit-tests/59636
        let pathbuf = PathBuf::from(OsString::from_vec(vec![255]));
        let path = pathbuf.as_path();
        let prefix = "";
        let error = upload_file(storage_config, &db_config, dataset_id, path, prefix)
            .await
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

    #[tokio::test]
    async fn test_bad_storage_config() {
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                "[blah]\naccess_key = \"whatever\"",
                config::FileFormat::Toml,
            ))
            .unwrap();

        let url_str =
            "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/src/resources/test.dat";
        let file_paths = vec![UploadedFile {
            dataset_id: Uuid::parse_str("d11cc371-f33b-4dad-ac2e-3c4cca30a256").unwrap(),
            created_date: Utc::now(),
            url: Url::parse(url_str).unwrap(),
            filesize: 12,
            version: "blah".to_owned(),
            metadata: json!({}),
        }];
        let error = download_files(config, file_paths)
            .await
            .expect_err("Missing storage config should error");
        assert!(
            error.to_string().contains("missing field"),
            "{}",
            error.to_string()
        );
    }
}
