//! Upload and download files to/from cloud storage.

use anyhow::Result;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use futures_core::stream::Stream;
use indicatif::{MultiProgress, ProgressBar};
use log::debug;
use read_progress_stream::ReadProgressStream;
use reqwest::Body;
use tokio_util::codec;

use super::{check_response, DatabaseApiConfig};
use crate::core::commands;

/// Get the md5 hash (for checksumming) of a file.
///
/// # Errors
///
/// Returns an error if reading the file fails.
pub async fn md5_file(path: &str) -> Result<String> {
    let tokio_file = tokio::fs::File::open(path).await?;
    // Feed file to md5 without reading whole file into RAM
    let md5_ctx = codec::FramedRead::new(tokio_file, codec::BytesCodec::new())
        .try_fold(md5::Context::new(), |mut ctx, chunk| async move {
            ctx.consume(chunk);
            Ok(ctx)
        })
        .await?;
    let md5_digest = md5_ctx.compute();
    let md5_bytes: [u8; 16] = md5_digest.into();
    let md5_str = format!("{:x}", md5_digest);
    debug!("Got md5 hash for {:?}: {}", path, md5_str);
    let encoded = base64::encode(md5_bytes);
    debug!("Base64-encoded md5 hash to: {}", encoded);
    Ok(encoded)
}

/// Upload a file to cloud storage in a single request.
///
/// Uses the [S3 PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
///
/// # Errors
///
/// Returns an error if reading the file fails.
///
/// Returns an error if cloud storage returns a non-200 response (e.g. if auth
/// credentials are invalid, if server is unreachable, if checksum doesn't
/// match) or if the returned data is malformed.
pub async fn upload_file(
    configuration: &DatabaseApiConfig,
    path: String,
    filesize: usize,
    key: String,
    multi_progress: &MultiProgress,
) -> Result<String> {
    debug!("building post request for: path={:?} key={:?}", path, key);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    // TODO: need to include bucket in path before key
    // TODO: add bucket to config file?
    api_url.set_path(&format!(
        "storage/v1/object/{}/{}",
        configuration.bucket, key
    ));
    let req_builder = client.post(api_url.as_str());

    let tokio_file = tokio::fs::File::open(&path).await?;
    let byte_stream =
        codec::FramedRead::new(tokio_file, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze());

    let progress_bar = multi_progress.add(ProgressBar::new(filesize as u64));
    progress_bar.set_style(commands::get_default_progress_bar_style());
    progress_bar.set_prefix(path);
    progress_bar.set_position(0);

    let pgbar = progress_bar.clone();
    // Let progress bar follow along with # bytes read
    let progress = Box::new(move |_bytes_read: u64, total_bytes_read: u64| {
        pgbar.set_position(total_bytes_read);
    });
    let read_wrapper = ReadProgressStream::new(byte_stream, progress);

    let req_builder = req_builder.body(Body::wrap_stream(read_wrapper));
    // TODO: add etag header? does it get passed thru storage-api?
    // doesn't look like it... open issue?

    let response = req_builder.send().await?;

    debug!("status: {}", response.status());
    let content: serde_json::Value = check_response(response).await?;
    debug!("content: {}", content);

    progress_bar.finish();
    Ok(key)
}

/// Download a file from cloud storage.
///
/// Uses the [S3 GetObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
///
/// # Errors
///
/// Returns an error if the url to download is malformed.
///
/// Returns an error if cloud storage returns a non-200 response (e.g. if auth
/// credentials are invalid, if server is unreachable, if checksum doesn't
/// match) or if the returned data is malformed.
pub async fn download_file(
    configuration: &DatabaseApiConfig,
    key: String,
) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
    debug!("building get request for: {:?}", key);
    let client = &configuration.client;

    let mut api_url = configuration.base_url.clone();
    api_url.set_path(&format!(
        "object/authenticated/{}/{}",
        configuration.bucket, key
    ));
    let req_builder = client.get(api_url.as_str());

    let response = req_builder.send().await?;

    Ok(response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
}

#[cfg(test)]
mod tests {
    // use super::*;
}
