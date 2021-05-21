// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::{anyhow, bail, Result};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::TryStreamExt;
use futures::stream::{try_unfold, Stream, StreamExt};
use log::debug;
use reqwest::Url;
use rusoto_core::{request, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, GetObjectRequest, PutObjectRequest, S3Client, StreamingBody,
    UploadPartRequest, S3,
};
use std::cmp::min;
use std::path::Path;
use tokio::fs::File;
// TODO: clean up imports
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec;

#[cfg(feature = "tangram-internal")]
use crate::app_config::DigitalOceanSpacesConfig;
use crate::app_config::{AwsS3Config, StorageProviderChoices};

pub struct StorageConfig {
    credentials: StaticProvider,
    bucket: String,
    region: Region,
}

impl StorageConfig {
    pub fn new(config: config::Config, provider: StorageProviderChoices) -> Result<StorageConfig> {
        match provider {
            #[cfg(feature = "tangram-internal")]
            StorageProviderChoices::DigitalOcean => {
                let do_config = config
                    .try_into::<DigitalOceanSpacesConfig>()?
                    .digitalocean_spaces;
                Ok(StorageConfig {
                    credentials: StaticProvider::new_minimal(
                        do_config.access_key,
                        do_config.secret_key,
                    ),
                    bucket: String::from("tangs-stage"),
                    region: Region::Custom {
                        name: "sfo2".to_owned(),
                        endpoint: "sfo2.digitaloceanspaces.com".to_owned(),
                        // TODO: use cdn endpoint for downloads?
                        // endpoint: "sfo2.cdn.digitaloceanspaces.com".to_owned(),
                    },
                })
            }
            StorageProviderChoices::Aws => {
                let aws_config = config.try_into::<AwsS3Config>()?.aws_s3;
                Ok(StorageConfig {
                    credentials: StaticProvider::new_minimal(
                        aws_config.access_key,
                        aws_config.secret_key,
                    ),
                    bucket: String::from("tangram-vision-datasets"),
                    region: Region::UsWest1,
                })
            }
        }
    }
}

// This could be sync, because we only call it on files for upload_file_oneshot,
// so the files will be small.
pub async fn md5_file(path: &Path) -> Result<String> {
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

// Async oneshot upload references
// https://github.com/softprops/elblogs/blob/96df314db92216a769dc92d90a5cb0ae42bb13da/src/main.rs#L212-L223
// https://stackoverflow.com/questions/57810173/streamed-upload-to-s3-with-rusoto
// https://github.com/rusoto/rusoto/issues/1771
// https://stackoverflow.com/questions/59318460/what-is-the-best-way-to-convert-an-asyncread-to-a-trystream-of-bytes
pub async fn upload_file_oneshot(
    config: StorageConfig,
    path: &Path,
    filesize: i64,
    key: String,
) -> Result<(Url, String)> {
    let region_endpoint = match &config.region {
        Region::Custom { endpoint, .. } => endpoint.clone(),
        r => format!("s3.{}.amazonaws.com", r.name()),
    };

    // Constructing url here to avoid borrow errors if we try to construct it at
    // the bottom of the function
    let url_str = format!("https://{}.{}/{}", config.bucket, region_endpoint, key);
    let url = Url::parse(&url_str)?;
    let md5_hash = md5_file(path).await?;

    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);

    let tokio_file = tokio::fs::File::open(path).await?;
    let bytemut_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new());
    let byte_stream = StreamingBody::new(bytemut_stream.map_ok(|bytes| bytes.freeze()));
    let req = PutObjectRequest {
        bucket: config.bucket,
        body: Some(byte_stream),
        // Required when body is a stream (will change for multipart upload)
        content_length: Some(filesize),
        content_md5: Some(md5_hash),
        key,
        ..Default::default()
    };
    debug!("making upload_file request {:?}", req);
    // just spawn tokio here and use it, instead of async-ing everything yet

    // https://www.rusoto.org/futures.html mentions turning futures into blocking calls
    let resp = client.put_object(req).await?;
    debug!("upload_file response {:?}", resp);
    let version = resp
        .version_id
        .ok_or_else(|| anyhow!("Uploaded file wasn't versioned by storage provider"))?;
    Ok((url, version))
}

#[derive(Debug)]
struct FileChunk {
    data: Vec<u8>,
    md5: String,
    part_number: i64,
}

#[derive(Debug)]
struct FileReadState<F>
where
    F: AsyncRead + AsyncReadExt + Unpin + Send,
{
    f: F,
    size_in_bytes: usize,
    // Track bytes remaining so we can size the buffer to match the last chunk,
    // since we're using read_exact to fill the buffer.
    remaining_bytes: usize,
    // Part number is i64 to match rusoto types
    part_number: i64,
}

fn read_file_chunks<F>(
    f: F,
    size_in_bytes: usize,
    filesize: usize,
) -> impl Stream<Item = Result<FileChunk, std::io::Error>> + Unpin + Send
where
    F: AsyncRead + AsyncReadExt + Unpin + Send + std::fmt::Debug,
{
    let seed = FileReadState {
        f,
        size_in_bytes,
        remaining_bytes: filesize,
        part_number: 1,
    };
    debug!(
        "Constructed unfold seed with filesize={}: {:?}",
        filesize, seed
    );
    Box::pin(try_unfold(seed, |mut state| async move {
        // f.read_exact fills the buffer, but throws UnexpectedEof if it reads
        // less than the size of the buffer, so we need to match the buffer size
        // to what we expect to read
        let bufsize = min(state.size_in_bytes, state.remaining_bytes);
        let mut buf = vec![0; bufsize];
        debug!(
            "remaining_bytes={} so allocated buffer of size={} for part number {}",
            state.remaining_bytes, bufsize, state.part_number
        );

        // Previously tried f.read, but it only returns 8KB at a time
        // Relevant? https://github.com/tokio-rs/tokio/issues/3694#issuecomment-826957113
        // match state.f.read(&mut buf).await {
        match state.f.read_exact(&mut buf).await? {
            0 => Ok(None),
            n => {
                debug!("Read n={} bytes from file {:?}", n, state.f);
                buf.resize(n, 0);
                let chunk = FileChunk {
                    // md5 could maybe be sped up by switching from read_exact
                    // to read and passing each chunk into md5::Context as it is
                    // read, instead of waiting for the full buffer to be read
                    // and then passing the full buffer to md5::compute.
                    //
                    // Or by making md5-calculation in a task, because right now
                    // it happens on the main thread, so we're limited to using
                    // 1 CPU core.
                    md5: base64::encode::<[u8; 16]>(md5::compute(&buf).into()),
                    data: buf,
                    part_number: state.part_number,
                };

                state.part_number += 1;
                state.remaining_bytes -= n;

                Ok(Some((chunk, state)))
            }
        }
    }))
}

async fn upload_completed_part(client: &S3Client, req: UploadPartRequest) -> Result<CompletedPart> {
    // TODO: add retry handling?
    // https://docs.rs/tokio-retry/0.3.0/tokio_retry/
    // TODO: count some number of retries
    let part_number = req.part_number;
    debug!("Making part {} upload_part request {:?}", part_number, req);
    let resp = client.upload_part(req).await;
    debug!("Result of part {} upload_part {:?}", part_number, resp);

    match resp {
        Ok(response) => {
            if let Some(e_tag) = response.e_tag {
                let part = CompletedPart {
                    e_tag: Some(e_tag),
                    part_number: Some(part_number),
                };
                Ok(part)
            } else {
                bail!(
                    "Response for upload part {} is missing ETag header!",
                    part_number
                );
            }
        }
        Err(e) => {
            debug!("Handling error in upload_completed_part: {}", e);
            // TODO: timeout error is encompassed by HttpDispatchError
            // https://github.com/rusoto/rusoto/issues/1530
            bail!("Upload part {} request failed: {}", part_number, e);
        }
    }
}

async fn upload_parts<F>(
    client: &S3Client,
    tokio_file: F,
    bucket: String,
    key: String,
    upload_id: &str,
    filesize: usize,
    // TODO: bundle these in a config object?
    chunk_size: usize,
    concurrent_request_limit: usize,
) -> Result<Vec<CompletedPart>>
where
    F: AsyncRead + AsyncReadExt + Unpin + Send + std::fmt::Debug,
{
    // TODO: Could this be simpler as tokio_file.
    let mut part_requests = read_file_chunks(tokio_file, chunk_size, filesize as usize).map_ok(
        |chunk: FileChunk| -> UploadPartRequest {
            // Prints vec of bytes:
            // debug!("Got chunk: {:?}", chunk);

            debug!(
                "Constructing chunk {} with data of size {}",
                chunk.part_number,
                chunk.data.len()
            );
            // let md5_hash = base64::encode::<[u8; 16]>(md5::compute(&chunk.data).into());
            let streaming_body = StreamingBody::from(chunk.data);
            let part_number = chunk.part_number;
            UploadPartRequest {
                body: Some(streaming_body),
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.to_owned(),
                content_md5: Some(chunk.md5),
                part_number,
                ..Default::default()
            }
        },
    );

    // The below async work could be changed to a functional approach, see:
    // https://gitlab.com/tangram-vision/bolster/-/merge_requests/10#note_581407198

    // Tokio threadpool spawns a thread per CPU and distributes tasks among
    // available threads, so tasks should be completed as fast as possible. We
    // use the concurrent_request_limit to limit how much of the file we read
    // into RAM at a time (having no limit leads to system freezes and
    // OOM-killing).
    let mut futs = FuturesUnordered::new();
    let mut parts: Vec<CompletedPart> = Vec::new();
    // Pool of S3Client clones that are checked-out and checked-in by each task.
    let mut client_pool: Vec<S3Client> = (0..concurrent_request_limit)
        .map(|_idx| client.clone())
        .collect();
    while let Some(maybe_req) = part_requests.next().await {
        if let Ok(req) = maybe_req {
            debug!("Sending req {} to task", req.part_number);
            if let Some(local_client) = client_pool.pop() {
                futs.push(tokio::spawn(async move {
                    debug!("Spawned task for req {}", req.part_number);
                    let part: CompletedPart = upload_completed_part(&local_client, req).await?;
                    Ok::<_, anyhow::Error>((part, local_client))
                }));
            } else {
                debug!("S3Client pool ran dry somehow!");
                bail!("S3Client pool ran dry somehow!");
            }

            // We won't handle errors from upload_completed_part until now.
            // TODO: Improve this, perhaps by moving task creation to a task and
            // awaiting on the union of the producer and consumer tasks.
            if futs.len() >= concurrent_request_limit {
                debug!("At concurrent_request_limit... awaiting a request finishing");
                // This won't return None because futs is not empty, so we can safely unwrap.
                // The ? operator can throw:
                //   - a JoinError (if the tokio::spawn task panics)
                //   - an error from upload_completed_part (i.e. making the upload_part request)
                let (part, local_client) = futs.next().await.unwrap()??;
                client_pool.push(local_client);
                debug!(
                    "Returning client to pool, current size = {}",
                    client_pool.len()
                );
                parts.push(part);
                debug!("Parts finished = {}", parts.len());
            }
        } else {
            debug!("Error reading file: {:?}", maybe_req);
            bail!("Error reading file: {:?}", maybe_req);
        }
    }
    debug!("All file chunks dispatched to tasks");
    while let Some(result) = futs.next().await {
        // The ? operator can throw:
        //   - a JoinError (if the tokio::spawn task panics)
        //   - an error from upload_completed_part (i.e. making the upload_part request)
        // Also, we don't care about returning S3Clients to the pool anymore.
        let (part, _) = result??;
        parts.push(part);
        debug!("Parts finished = {}", parts.len());
    }

    Ok(parts)
}

// S3 has some limits for multipart uploads: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
// Part numbers can go from 1-10,000
// Max object size is 5TB
// Part sizes can be between 5MB - 5GB
// Requests only return 1000 parts at a time
//
// Given these limits, we need to pick a chunk size. We can't just always pick
// 5MB, because then we could only upload files up to 5MB * 10000 parts = 50GB.
// We don't want to always pick 500MB, because then if you're uploading a 1GB
// file and hit an upload failure, you need to re-upload half of the file,
// whereas if the chunk size had been 5MB then you'd have to reupload very
// little.
//
// Also, I dislike using the full 10,000 parts because then you need to
// implement pagination to use the ListParts API. Also, fitting 10,000 parts
// into the CompleteMultipartUpload request makes it a big, slow request!
//
// So, we'll limit ourselves to 1000 parts and scale the part/chunk size along
// with the filesize so that we use small chunks for small files (so upload
// errors lose little progress) but we can still accommodate files up to the 5TB
// limit, which people will hopefully use good/stable internet to upload.
//
// One final consideration: Small chunk sizes mean we spend more time on the
// overhead of making requests and waiting for responses. So, we'll avoid 5MB
// chunks and (somewhat arbitrarily) pick a larger default chunk size of 16MB.
// When we provide resumable-upload functionality (or learn that users have
// slow/spotty internet), it may make sense to reduce this default chunk size or
// make it configurable.
//
// So, for files from 16MB up to 16GB, we will use 16MB chunks and 1-1000 parts.
// For files above 16GB, we start increasing the chunk size (ceiling'd to the
// nearest MB). We cap out at 5000GB (4.88TB).
const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const MEGABYTE: usize = 1024 * 1024;
const GIGABYTE: usize = 1024 * MEGABYTE;
const MAX_FILE_SIZE: usize = 5000 * GIGABYTE;
fn derive_chunk_size(filesize: usize) -> Result<usize> {
    if filesize > MAX_FILE_SIZE {
        bail!("File is too large to upload! Limit is {}", MAX_FILE_SIZE);
    }
    let filesize_mb = (filesize as f64) / (MEGABYTE as f64);
    let chunk_size_mb_for_1000_parts = (filesize_mb / 1000.0).ceil() as usize;
    Ok(std::cmp::max(
        DEFAULT_CHUNK_SIZE,
        chunk_size_mb_for_1000_parts * MEGABYTE,
    ))
}

// Multipart upload references
// https://docs.rs/s3-ext/0.2.2/s3_ext/trait.S3Ext.html#tymethod.upload_from_file_multipart
// https://stackoverflow.com/questions/66558012/rust-aws-multipart-upload-using-rusoto-multithreaded-rayon-panicked-at-there
// https://gist.github.com/ivormetcalf/f2b8e6abfece4328c86ad1ee34363caf
pub async fn upload_file_multipart(
    config: StorageConfig,
    path: &Path,
    filesize: usize,
    key: String,
) -> Result<(Url, String)> {
    let region_endpoint = match &config.region {
        Region::Custom { endpoint, .. } => endpoint.clone(),
        r => format!("s3.{}.amazonaws.com", r.name()),
    };

    let url_str = format!("https://{}.{}/{}", config.bucket, region_endpoint, key);
    let url = Url::parse(&url_str)?;

    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);

    // ======
    // Create multipart upload (to get the upload_id)
    // ======
    let req = CreateMultipartUploadRequest {
        bucket: config.bucket.clone(),
        key: key.clone(),
        ..Default::default()
    };
    debug!("Making create_multipart_upload request {:?}", req);
    let resp = client.create_multipart_upload(req).await?;
    debug!("Result of create_multipart_upload {:?}", resp);
    let upload_id = resp
        .upload_id
        .ok_or_else(|| anyhow!("Multipart upload is missing an UploadId"))?;

    // ======
    // Upload parts
    // ======
    // TODO: Make concurrent_request_limit (or RAM usage) configurable
    const CONCURRENT_REQUEST_LIMIT: usize = 30;

    let tokio_file = tokio::fs::File::open(path).await?;
    let completed_parts = upload_parts(
        &client,
        tokio_file,
        config.bucket.clone(),
        key.clone(),
        &upload_id,
        filesize,
        derive_chunk_size(filesize)?,
        CONCURRENT_REQUEST_LIMIT,
    )
    .await?;

    // ======
    // Complete multipart upload
    // ======
    let req = CompleteMultipartUploadRequest {
        bucket: config.bucket.clone(),
        key: key.clone(),
        upload_id,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(completed_parts),
        }),
        ..Default::default()
    };
    debug!("Making complete_multipart_upload request {:?}", req);
    let resp = client.complete_multipart_upload(req).await?;
    debug!("Result of complete_multipart_upload {:?}", resp);
    // resp.location is s3.us-west-1.amazonaws.com/tangram-vision-datasets/
    // whereas url is tangram-vision-datasets.s3.us-west-1.amazonaws.com/
    // So they won't match, but we can just use the url value.
    let version = resp
        .version_id
        .ok_or_else(|| anyhow!("Uploaded file wasn't versioned by storage provider"))?;
    debug!("Resulting version {}", version);

    Ok((url, version))
}

#[tokio::main]
pub async fn download_file(config: StorageConfig, url: &Url) -> Result<()> {
    // TODO: Is there a better way to do this, like how try_from works for getting upload config?

    // TODO: store provider, bucket, and key separately in database?
    let key = url
        .path()
        .strip_prefix("/")
        .ok_or_else(|| anyhow!("URL path didn't start with /: {}", url.path()))?;
    let filename = key
        .split('/')
        .last()
        .ok_or_else(|| anyhow!("Key can't become filename: {}", key))?;

    // Increase read buffer size in rusoto:
    // https://www.rusoto.org/performance.html
    // TODO: test the effect of this change!
    let mut http_config = request::HttpConfig::new();
    http_config.read_buf_size(2 * 1024 * 1024);
    let dispatcher = request::HttpClient::new_with_config(http_config).unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);
    let req = GetObjectRequest {
        bucket: config.bucket,
        key: key.to_owned(),
        ..Default::default()
    };
    debug!("making download_file request {:?}", req);

    let resp = client.get_object(req).await?;
    debug!("download_file response {:?}", resp);

    let body = resp.body.ok_or_else(|| anyhow!("Empty file! {}", url))?;
    let mut body = body.into_async_read();
    let mut file = File::create(filename).await?;
    io::copy(&mut body, &mut file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use predicates::prelude::*;
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};
    use tokio_test::io::Builder;

    #[test]
    fn test_download_file_403_forbidden() {
        // To debug what rusoto and httpmock are doing, enable logger and run
        // tests with debug or trace level.
        // let _ = env_logger::try_init();

        let bucket = "tangram-test".to_owned();
        let key = "test-file";
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(format!("/{}/{}", bucket, key));
            then.status(403).body("AccessDenied");
            // Rusoto doesn't seem to parse the error xml anyway, so just use the simpler response body
            // .body(r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><BucketName>tangs-stage</BucketName><RequestId>tx00000000000001970993c-0060245383-5ed52e8-sfo2a</RequestId><HostId>5ed52e8-sfo2a-sfo</HostId></Error>"#);
        });
        let test_region = Region::Custom {
            name: "test".to_owned(),
            endpoint: server.base_url(),
        };
        let url_str = format!("{}/{}", server.base_url(), key);
        let url = Url::parse(&url_str).unwrap();

        let config = StorageConfig {
            credentials: StaticProvider::new_minimal("abc".to_owned(), "def".to_owned()),
            region: test_region,
            bucket,
        };

        let error = download_file(config, &url).expect_err("403 Forbidden response expected");
        match error.downcast_ref::<rusoto_core::RusotoError<rusoto_s3::GetObjectError>>() {
            Some(rusoto_core::RusotoError::Unknown(b)) => assert_eq!(b.status, 403),
            e => panic!("Unexpected error: {:?}", e),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_read_file_chunks() {
        let mock_string = String::from("test");
        let reader = Builder::new().read(mock_string.as_bytes()).build();

        let chunk_size = 2;
        let filesize = 4;

        let expected_parts: [i64; 2] = [1, 2];
        let expected_chunks = vec!["te".as_bytes(), "st".as_bytes()];

        let mut s = read_file_chunks(reader, chunk_size, filesize);
        let mut i = 0;
        while let Some(item) = s.next().await {
            let item = item.expect("Did not receive a valid chunk.");
            assert_eq!(item.part_number, expected_parts[i]);
            assert_eq!(item.data.as_slice(), expected_chunks[i]);
            i += 1;
        }
        assert_eq!(i, 2);
    }

    #[tokio::test]
    async fn test_read_file_chunks_odd_last_chunk() {
        let mock_string = String::from("test1");
        let reader = Builder::new().read(mock_string.as_bytes()).build();

        let chunk_size = 2;
        let filesize = 5;

        let expected_parts: [i64; 3] = [1, 2, 3];
        let expected_chunks = vec!["te".as_bytes(), "st".as_bytes(), "1".as_bytes()];

        let mut s = read_file_chunks(reader, chunk_size, filesize);
        let mut i = 0;
        while let Some(item) = s.next().await {
            let item = item.expect("Did not receive a valid chunk.");
            assert_eq!(item.part_number, expected_parts[i]);
            assert_eq!(item.data.as_slice(), expected_chunks[i]);
            i += 1;
        }
        assert_eq!(i, 3);
    }

    #[tokio::test]
    async fn test_read_file_chunks_error_reading() {
        let reader = Builder::new()
            .read_error(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "damn",
            ))
            .build();

        let chunk_size = 2;
        let filesize = 8;
        let mut s = read_file_chunks(reader, chunk_size, filesize);
        if let Some(item) = s.next().await {
            assert!(
                item.is_err(),
                "Expected first read chunk to be an err and it wasn't. Full chunk: {}",
                item.unwrap_err()
            );
        }
    }

    #[tokio::test]
    async fn test_read_file_chunks_error_exits_early() {
        // I switched read_file_chunks from unfold to try_unfold, so now the
        // stream should exit early with an error if it encounters one, rather
        // than continuing to read the rest of the file.
        let _ = env_logger::try_init();

        let reader = Builder::new()
            .read_error(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "damn",
            ))
            .build();

        let chunk_size = 8;
        let filesize = 10;
        let mut part_requests = read_file_chunks(reader, chunk_size, filesize);

        let r1 = part_requests
            .try_next()
            .await
            .expect_err("Expected error on first read");
        assert!(predicate::str::contains("damn").eval(&r1.to_string()));
        let r2 = part_requests
            .try_next()
            .await
            .expect("Expected Ok(None) from subsequent reads");
        assert!(r2.is_none());
        let r3 = part_requests
            .try_next()
            .await
            .expect("Expected Ok(None) from subsequent reads");
        assert!(r3.is_none());
    }

    #[tokio::test]
    async fn test_read_file_chunks_read_smaller_than_chunk() {
        let mock_string = String::from("ohno");
        let reader = Builder::new()
            .read(mock_string.as_bytes())
            .read(mock_string.as_bytes())
            .build();

        let chunk_size = 6;
        let filesize = 8;

        let expected_parts: [i64; 2] = [1, 2];
        let expected_chunks = vec!["ohnooh".as_bytes(), "no".as_bytes()];

        let mut s = read_file_chunks(reader, chunk_size, filesize);
        let mut i = 0;
        while let Some(item) = s.next().await {
            let item = item.expect("Did not receive a valid chunk.");
            assert_eq!(item.part_number, expected_parts[i]);
            assert_eq!(item.data.as_slice(), expected_chunks[i]);
            i += 1;
        }
        assert_eq!(i, 2);
    }

    #[tokio::test]
    async fn test_upload_completed_part_success() {
        let _ = env_logger::try_init();

        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default()
                .with_body("blah")
                .with_header("ETag", "testvalue"),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let req = UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        };
        let part = upload_completed_part(&client, req).await.unwrap();
        assert_eq!(
            part,
            CompletedPart {
                e_tag: Some("testvalue".to_owned()),
                part_number: Some(1)
            }
        );
    }

    #[tokio::test]
    async fn test_upload_completed_part_missing_etag() {
        let _ = env_logger::try_init();

        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default().with_body("blah"),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let req = UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        };
        let e = upload_completed_part(&client, req)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            true,
            predicate::str::contains("Response for upload part 1 is missing ETag header!").eval(&e)
        );
    }

    #[tokio::test]
    async fn test_upload_completed_part_timeout() {
        let _ = env_logger::try_init();

        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::with_dispatch_error(
                rusoto_core::request::HttpDispatchError::new("my timeout message".to_owned()),
            ),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let req = UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        };

        // First request will fail with HttpDispatchError (can indicate a timeout).
        // Function should retry and succeed on second request.
        let e = upload_completed_part(&client, req)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            true,
            predicate::str::contains("my timeout message").eval(&e)
        );
    }

    #[tokio::test]
    async fn test_upload_parts_file_read_err_exits_early() {
        // Error reading file throws immediately
        let _ = env_logger::try_init();

        let reader = Builder::new()
            .read("ohno".as_bytes())
            .read_error(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Reading file failed",
            ))
            .build();

        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default().with_body("blah"),
            MockCredentialsProvider,
            Default::default(),
        );

        let e = upload_parts(
            &client,
            reader,
            "test".to_owned(),
            "test".to_owned(),
            "test",
            8,
            4,
            2,
        )
        .await
        .unwrap_err()
        .to_string();
        assert_eq!(
            true,
            predicate::str::contains("Reading file failed").eval(&e)
        );
    }

    #[tokio::test]
    async fn test_upload_parts_network_err_exits_early() {
        // Error reading file throws immediately
        let _ = env_logger::try_init();

        let reader = Builder::new()
            .read("ohno".as_bytes())
            .read("ohno".as_bytes())
            .read("ohno".as_bytes())
            .build();

        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::with_dispatch_error(
                rusoto_core::request::HttpDispatchError::new("my timeout message".to_owned()),
            ),
            MockCredentialsProvider,
            Default::default(),
        );

        let e = upload_parts(
            &client,
            reader,
            "test".to_owned(),
            "test".to_owned(),
            "test",
            12,
            4,
            // concurrent_request_limit must be >= num_chunks to exhaust the
            // reader mock before the network error is handled, otherwise the
            // mock panics with "There is still data left to read"
            4,
        )
        .await
        .unwrap_err()
        .to_string();
        assert_eq!(
            true,
            predicate::str::contains("my timeout message").eval(&e)
        );
    }

    #[test]
    fn test_derive_chunk_size() {
        assert_eq!(
            derive_chunk_size(DEFAULT_CHUNK_SIZE + 1).unwrap(),
            DEFAULT_CHUNK_SIZE
        );
        assert_eq!(
            derive_chunk_size(DEFAULT_CHUNK_SIZE * 1000).unwrap(),
            DEFAULT_CHUNK_SIZE
        );
        assert_eq!(
            derive_chunk_size(DEFAULT_CHUNK_SIZE * 1000 + 1).unwrap(),
            DEFAULT_CHUNK_SIZE + MEGABYTE
        );
        assert_eq!(
            derive_chunk_size((DEFAULT_CHUNK_SIZE + MEGABYTE) * 1000).unwrap(),
            DEFAULT_CHUNK_SIZE + MEGABYTE
        );
        assert_eq!(
            // 5 TB (almost)
            derive_chunk_size(5000 * GIGABYTE).unwrap(),
            5 * GIGABYTE
        );

        let e = derive_chunk_size(5001 * GIGABYTE).unwrap_err().to_string();
        assert_eq!(
            true,
            predicate::str::contains("File is too large to upload").eval(&e)
        );
    }

    // TODO: test that errors coming out of upload_completed_part actually error out of the upload process (stop all workers/tasks)

    // TODO: test create_multipart_upload failing with Credentials type RusotoError
    // https://docs.rs/rusoto_core/0.46.0/rusoto_core/enum.RusotoError.html

    // TODO: test if maybe_chunk is Err
}
