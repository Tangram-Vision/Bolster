//! High-level operations that roughly align with CLI subcommands.
//!
//! For overall architecture, see [ARCHITECTURE.md](https://gitlab.com/tangram-vision-oss/bolster/-/blob/main/ARCHITECTURE.md)

use std::{convert::TryInto, iter, sync::Arc};

use anyhow::Result;
use byte_unit::MEBIBYTE;
use futures::{stream, stream::StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::debug;
use read_progress_stream::ReadProgressStream;
use reqwest::Url;
use serde_json::json;
use uuid::Uuid;

use super::{
    api::{
        datasets::{self, DatabaseApiConfig, DatasetGetRequest},
        storage,
        storage::StorageConfig,
    },
    models::{Dataset, UploadedFile},
};
use crate::app_config::CompleteAppConfig;

/// Number of files allowed to upload at the same time.
pub const MAX_FILES_UPLOADING_CONCURRENTLY: usize = 4;

/// Number of files allowed to download at the same time.
pub const MAX_FILES_DOWNLOADING_CONCURRENTLY: usize = 4;

/// Files with sizes under this threshold use one-shot upload, all other files
/// use multipart upload.
///
/// The threshold is set to 64MB (currently yielding 4x 16MB part uploads).
/// The threshold is set a bit arbitrarily -- it is above 64MB that the
/// multipart upload starts being faster than one-shot uploads in local testing.
/// Below 64MB, the extra overhead of extra API calls makes multipart uploads
/// slower.
pub const MULTIPART_FILESIZE_THRESHOLD: usize = 64 * (MEBIBYTE as usize);

/// Provides the default progress bar style
///
/// For a list of template fields (e.g. elapsed time, bytes remaining), see
/// [indicatif's documentation on
/// Templates](https://docs.rs/indicatif/0.16.2/indicatif/#templates).
pub fn get_default_progress_bar_style() -> ProgressStyle {
    ProgressStyle::default_bar()
    .template("{prefix} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ({eta})")
    .progress_chars("#>-")
}

/// Creates a dataset and returns its id.
///
/// Thin wrapper around [datasets::datasets_post] -- see its documentation for
/// behavior and possible errors.
pub async fn create_dataset(config: &DatabaseApiConfig, device_id: String) -> Result<Uuid> {
    let dataset = datasets::datasets_post(config, device_id, json!({})).await?;
    Ok(dataset.dataset_id)
}

/// Eases usage of [multiple progress
/// bars](https://docs.rs/indicatif/0.16.2/indicatif/struct.MultiProgress.html)
/// in an async environment.
///
/// Manages annoyances with indicatif, namely that:
/// - some thread of execution needs to join the MultiProgress to get progress
/// bars to render
/// - joining the MultiProgress immediately returns if there aren't ProgressBars
/// attached, so we add a hidden/bogus one
/// - the hidden/bogus ProgressBar needs to be cleaned up (by Drop, in this
/// implementation) when we don't need to update progress bars anymore
pub struct MultiProgressGuard {
    /// Pointer to the multi-progress bar, cloned internally and passed to a
    /// tokio task to join to the bar so it renders.
    inner: Arc<MultiProgress>,
    /// Hidden spinner progress bar to ensure the multi-progress bar stays alive
    /// until this guard is dropped.
    hidden_spinner: ProgressBar,
}

impl MultiProgressGuard {
    /// Initializes a
    /// [MultiProgress](https://docs.rs/indicatif/0.16.2/indicatif/struct.MultiProgress.html)
    /// (with a hidden progress bar) and joins it to begin rendering.
    pub async fn new() -> Self {
        let mp = Arc::new(MultiProgress::new());
        let spinner = mp.add(ProgressBar::hidden());
        let guard = MultiProgressGuard {
            inner: mp,
            hidden_spinner: spinner,
        };
        let mp2 = guard.inner.clone();
        tokio::spawn(async move {
            mp2.join().unwrap();
        });
        guard
    }
}

impl Drop for MultiProgressGuard {
    fn drop(&mut self) {
        // Calling `spinner.finish` makes it appear for some reason, so we use
        // `finish_and_clear` instead.
        self.hidden_spinner.finish_and_clear();
    }
}

/// Creates a dataset and async uploads all provided files.
///
/// See [Performance][crate#performance] for details on upload concurrency.
///
/// Wraps [create_dataset] and [upload_file] -- see those functions for behavior
/// and possible errors.
pub async fn create_and_upload_dataset(
    config: StorageConfig,
    db_config: &DatabaseApiConfig,
    device_id: String,
    prefix: &str,
    file_paths: Vec<String>,
) -> Result<()> {
    let dataset_id: Uuid = create_dataset(db_config, device_id).await?;

    println!("Created new dataset with UUID: {}", dataset_id);
    debug!("paths: {:?}", file_paths);

    let guard = MultiProgressGuard::new().await;
    let multi_progress = guard.inner.clone();

    let mut futs = stream::iter(file_paths.into_iter().map(|path| {
        // Uploads to storage AND registers to database
        upload_file(
            config.clone(),
            db_config,
            dataset_id,
            path,
            prefix,
            &multi_progress,
        )
    }))
    .buffer_unordered(MAX_FILES_UPLOADING_CONCURRENTLY);
    while let Some(res) = futs.next().await {
        res?;
    }

    // After all uploads are complete, notify the backend so it can begin
    // processing, send notifications, etc.
    debug!("Upload(s) complete, notifying backend of completion");
    datasets::datasets_notify_upload_complete(db_config, dataset_id).await?;

    Ok(())
}

/// List all datasets, optionally filtered by options in [DatasetGetRequest].
///
/// Thin wrapper around [datasets::datasets_get] -- see its documentation for
/// behavior and possible errors.
pub async fn list_datasets(
    config: &DatabaseApiConfig,
    params: &DatasetGetRequest,
) -> Result<Vec<Dataset>> {
    let datasets = datasets::datasets_get(config, params).await?;

    Ok(datasets)
}

/// Registers uploaded file (critically, its url) in the datasets database.
///
/// Thin wrapper around [datasets::files_post] -- see its documentation for
/// behavior and possible errors.
pub async fn add_file_to_dataset(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    url: &Url,
    filesize: usize,
    version: String,
    metadata: serde_json::Value,
) -> Result<()> {
    datasets::files_post(config, dataset_id, url, filesize, version, metadata).await?;
    Ok(())
}

/// Uploads a single file at the given path to the cloud storage provider
/// indicated in `config` and registers the uploaded file in the datasets
/// database.
///
/// Folder structure is preserved when uploading, so uploading `dir/file` is
/// different from doing `cd dir` then uploading `file`.
///
/// Dispatches to [storage::upload_file_oneshot] if the file is < 64 MB or
/// [storage::upload_file_multipart] otherwise.
///
/// # Errors
///
/// Returns an error if the file is unreadable.
///
/// Invokes [storage::upload_file_oneshot], [storage::upload_file_multipart],
/// and [add_file_to_dataset] -- see those functions' documentation for
/// additional behavior and possible errors.
pub async fn upload_file(
    config: StorageConfig,
    db_config: &DatabaseApiConfig,
    dataset_id: Uuid,
    path: String,
    prefix: &str,
    multi_progress: &MultiProgress,
) -> Result<()> {
    // We retain any directories in the path
    let key = format!("{}/{}/{}", prefix, dataset_id, path);
    debug!("key {}", key);

    debug!("Got path {:?}", path);
    let filesize: usize = tokio::fs::metadata(path.clone())
        .await?
        .len()
        .try_into()
        .unwrap();

    let metadata = json!({});

    if filesize < MULTIPART_FILESIZE_THRESHOLD {
        debug!(
            "Filesize {} < threshold {} so doing oneshot",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) =
            storage::upload_file_oneshot(config, path, filesize, key, &multi_progress).await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata).await?;
    } else {
        debug!(
            "Filesize {} > threshold {} so doing multipart",
            filesize, MULTIPART_FILESIZE_THRESHOLD
        );
        let (url, version) =
            storage::upload_file_multipart(config, path, filesize as usize, key, &multi_progress)
                .await?;
        // Register uploaded file to database
        add_file_to_dataset(&db_config, dataset_id, &url, filesize, version, metadata).await?;
    }

    Ok(())
}

/// List all files in the given dataset, optionally filtered by prefixes.
///
/// If multiple prefixes are provided, all files matching any prefix are
/// returned (i.e. it's a union).
///
/// Thin wrapper around [datasets::files_get] -- see its documentation for
/// behavior and possible errors.
pub async fn list_files(
    config: &DatabaseApiConfig,
    dataset_id: Uuid,
    prefixes: Vec<String>,
) -> Result<Vec<UploadedFile>> {
    datasets::files_get(config, dataset_id, prefixes).await
}

/// Download all files specified in `uploaded_files`.
///
/// See [Performance][crate#performance] for details on download concurrency.
///
/// # Errors
///
/// Returns an error if the url doesn't match a configured cloud storage provider.
///
/// Wraps [download_file] -- see its documentation for other possible errors.
pub async fn download_files(
    storage_config: StorageConfig,
    uploaded_files: Vec<UploadedFile>,
) -> Result<()> {
    if uploaded_files.is_empty() {
        Ok(())
    } else {
        let guard = MultiProgressGuard::new().await;
        let multi_progress = guard.inner.clone();

        let mut futs = stream::iter(
            uploaded_files
                .iter()
                .zip(iter::repeat_with(|| storage_config.clone()))
                .map(|(uploaded_file, local_storage_config)| {
                    download_file(local_storage_config, &uploaded_file, &multi_progress)
                }),
        )
        .buffer_unordered(MAX_FILES_DOWNLOADING_CONCURRENTLY);
        while let Some(res) = futs.next().await {
            res?;
        }

        Ok(())
    }
}

/// Downloads a single file.
///
/// Folder structure is preserved when downloading, so downloading `dir/file`
/// will create a folder named `dir` (if it doesn't already exist) and download
/// `file` into that folder.
///
/// # Errors
///
/// Returns an error if the url is malformed or if the destination file cannot
/// be opened or written.
///
/// Wraps [storage::download_file] -- see its documentation for other possible
/// errors.
pub async fn download_file(
    storage_config: StorageConfig,
    uploaded_file: &UploadedFile,
    multi_progress: &MultiProgress,
) -> Result<()> {
    debug!("Downloading file: {}", uploaded_file.url);
    let filepath = uploaded_file.filepath_from_url()?;
    if let Some(dir) = filepath.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }

    let progress_bar = multi_progress.add(ProgressBar::new(uploaded_file.filesize));
    progress_bar.set_style(get_default_progress_bar_style());
    progress_bar.set_prefix(filepath.to_string_lossy().into_owned());
    progress_bar.set_position(0);
    let pgbar = progress_bar.clone();
    // Let progress bar follow along with # bytes read
    let progress = Box::new(move |_bytes_read: u64, total_bytes_read: u64| {
        pgbar.set_position(total_bytes_read);
    });

    let async_data = storage::download_file(storage_config, &uploaded_file.url).await?;
    let mut file = tokio::fs::File::create(filepath.clone()).await?;
    let read_wrapper = ReadProgressStream::new(async_data, progress);

    let mut wrapper = tokio_util::io::StreamReader::new(read_wrapper);
    tokio::io::copy(&mut wrapper, &mut file).await?;
    debug!("Downloaded file copied to destination: {:?}", filepath);
    progress_bar.finish();

    Ok(())
}

/// Show current configuration.
pub fn print_config(config: config::Config) -> Result<()> {
    let storage_config: CompleteAppConfig = config.try_into()?;
    println!("{}", toml::to_string(&storage_config)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{
        app_config::{DatabaseConfig, StorageProviderChoices},
        core::api::datasets::DatabaseApiConfig,
    };

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
        let path = "nonexistent-file".to_owned();
        let prefix = "";
        let mp = MultiProgress::new();
        let error = upload_file(storage_config, &db_config, dataset_id, path, prefix, &mp)
            .await
            .expect_err("Loading nonexistent file should fail");
        assert!(
            error.to_string().contains("No such file or directory"),
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

        let url_str =
            "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/src/resources/test.dat";
        let uploaded_files = vec![UploadedFile {
            dataset_id: Uuid::parse_str("d11cc371-f33b-4dad-ac2e-3c4cca30a256").unwrap(),
            created_date: Utc::now(),
            url: Url::parse(url_str).unwrap(),
            filesize: 12,
            version: "blah".to_owned(),
            metadata: json!({}),
        }];

        // Based on url from database, find which StorageProvider's config to use
        let provider = StorageProviderChoices::from_url(&uploaded_files[0].url).unwrap();
        let error =
            StorageConfig::new(config, provider).expect_err("Missing storage config should error");
        assert!(
            error
                .to_string()
                .contains("Config file must contain a [aws_s3] section"),
            "{}",
            error.to_string()
        );
    }
}
