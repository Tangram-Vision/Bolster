// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::{anyhow, bail, Result};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::TryStreamExt;
use futures::stream::{unfold, Stream, StreamExt};
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
use std::sync::{Arc, Mutex};
use tokio::fs::File;
use tokio::sync::mpsc;
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
    Box::pin(unfold(seed, |mut state| async move {
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
        match state.f.read_exact(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                debug!("Read n={} bytes from file {:?}", n, state.f);
                buf.resize(n, 0);
                let chunk = FileChunk {
                    data: buf,
                    part_number: state.part_number,
                };

                state.part_number += 1;
                state.remaining_bytes -= n;

                Some((Ok(chunk), state))
            }
            // TODO: test different kinds of io errors, be able to disambiguate EOF which isn't really an error
            Err(e) => Some((Err(e), state)),
        }
    }))
}

async fn upload_completed_part(
    client: &S3Client,
    maybe_req: std::io::Result<UploadPartRequest>,
    completed_parts: &Arc<Mutex<Vec<CompletedPart>>>,
) -> Result<()> {
    match maybe_req {
        Ok(req) => {
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
                        // TODO: send completed parts back on channel or use mutex?
                        let mut cp = completed_parts.lock().unwrap();
                        cp.push(CompletedPart {
                            e_tag: Some(e_tag),
                            part_number: Some(part_number),
                        });
                    } else {
                        bail!(
                            "Response for upload part {} is missing ETag header!",
                            part_number
                        );
                    }
                }
                Err(e) => {
                    // TODO: timeout error is encompassed by HttpDispatchError
                    // https://github.com/rusoto/rusoto/issues/1530
                    bail!("Upload part {} request failed: {}", part_number, e);
                }
            }
        }
        Err(e) => {
            debug!("part_request error {:?}", e);
            bail!("Got err UploadPartRequest: {}", e);
            // TODO: Log error

            // TODO: Send cancellation req to S3? or just let it expire

            // TODO: send cancellation request to main thread?
            // TODO: change to try_unfold when reading chunks so failures exit early
            // TODO: change to try_unfold when reading chunks so failures exit early
            // TODO: change to try_unfold when reading chunks so failures exit early
            // break;
        }
    }
    Ok(())
}

// Multipart upload references
// https://docs.rs/s3-ext/0.2.2/s3_ext/trait.S3Ext.html#tymethod.upload_from_file_multipart
// https://stackoverflow.com/questions/66558012/rust-aws-multipart-upload-using-rusoto-multithreaded-rayon-panicked-at-there
// https://gist.github.com/ivormetcalf/f2b8e6abfece4328c86ad1ee34363caf
pub async fn upload_file_multipart(
    config: StorageConfig,
    path: &Path,
    filesize: i64,
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
        // TODO: submit Content-MD5 also
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
    let tokio_file = tokio::fs::File::open(path).await?;

    // TODO: determine chunk size based on file size, something like:
    // chunk_size = max(25MB, ceil(filesize / 1000))
    // after 25GB file size, all uploads use 1000 parts
    // Could use more parts, but 10_000 etags in the complete_multipart_upload request seems excessive
    // discussion: https://stackoverflow.com/a/46564791
    const CHUNK_SIZE: usize = 5 * 1024 * 1024;

    let bucket = config.bucket.clone();
    // TODO: Could this be simpler as tokio_file.
    let mut part_requests = read_file_chunks(tokio_file, CHUNK_SIZE, filesize as usize).map(
        |maybe_chunk| -> Result<UploadPartRequest, std::io::Error> {
            // Prints vec of bytes:
            // debug!("Got maybe_chunk: {:?}", maybe_chunk);
            let chunk = maybe_chunk?;

            debug!(
                "Constructing chunk {} with data of size {}",
                chunk.part_number,
                chunk.data.len()
            );
            let streaming_body = StreamingBody::from(chunk.data);
            let part_number = chunk.part_number;
            Ok(UploadPartRequest {
                body: Some(streaming_body),
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                part_number,
                ..Default::default()
            })
        },
    );
    debug!("Prepared file chunk stream, mapped to UploadPartRequests");

    // TODO: Make num_workers configurable
    const NUM_WORKERS: usize = 10;
    let mut work_senders = vec![];
    let mut work_receivers = vec![];
    // TODO: change from mutex to message on channel back to main thread?
    let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..NUM_WORKERS {
        // TODO: bounds of channel determines how much RAM we use... make it configurable
        let (sender, receiver) = mpsc::channel(10);
        work_senders.push(sender);
        work_receivers.push(receiver);
    }

    let futures = FuturesUnordered::new();
    for mut receiver in work_receivers {
        let worker_client = client.clone();
        let worker_completed_parts = completed_parts.clone();
        let job = tokio::spawn(async move {
            while let Some(maybe_req) = receiver.recv().await {
                // TODO: provide a channel for returning errors on instead of doing unwrap here
                upload_completed_part(&worker_client, maybe_req, &worker_completed_parts)
                    .await
                    .unwrap();
            }
        });
        futures.push(job);
    }

    let mut i = 0;
    while let Some(maybe_req) = part_requests.next().await {
        debug!("Sending job {} to worker", i);
        work_senders[i % NUM_WORKERS].send(maybe_req).await?;
        // for unbounded: work_senders[i % NUM_WORKERS].send(maybe_req)?;
        i += 1;
    }
    // Drop all channel Senders so Receivers will stop waiting for jobs
    work_senders.clear();

    // Block until all requests sent
    debug!("Waiting on futures.collect of {:?}", futures);
    futures.collect::<Vec<_>>().await;

    // ======
    // Create multipart upload (to get the upload_id)
    // ======
    let completed_parts_mutex: Mutex<Vec<CompletedPart>> = Arc::try_unwrap(completed_parts)
        .map_err(|err| anyhow!("Failed to unwrap Arc of completed_parts: {:?}", err))?;
    let completed_parts = completed_parts_mutex.into_inner()?;
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

        let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default()
                .with_body("blah")
                .with_header("ETag", "testvalue"),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let maybe_req = Ok(UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        });
        upload_completed_part(&client, maybe_req, &completed_parts)
            .await
            .unwrap();
        let parts = completed_parts.lock().unwrap();
        debug!("Parts length: {}", parts.len());
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0],
            CompletedPart {
                e_tag: Some("testvalue".to_owned()),
                part_number: Some(1)
            }
        );
    }

    #[tokio::test]
    async fn test_upload_completed_part_missing_etag() {
        let _ = env_logger::try_init();

        let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default().with_body("blah"),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let maybe_req = Ok(UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        });
        let e = upload_completed_part(&client, maybe_req, &completed_parts)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            true,
            predicate::str::contains("Response for upload part 1 is missing ETag header!").eval(&e)
        );
        assert_eq!(completed_parts.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_upload_completed_part_timeout() {
        let _ = env_logger::try_init();

        let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::with_dispatch_error(
                rusoto_core::request::HttpDispatchError::new("my timeout message".to_owned()),
            ),
            MockCredentialsProvider,
            Default::default(),
        );
        let body: Vec<u8> = vec![1, 2, 3];
        let maybe_req = Ok(UploadPartRequest {
            body: Some(StreamingBody::from(body)),
            bucket: "test".to_owned(),
            key: "test".to_owned(),
            upload_id: "test".to_owned(),
            part_number: 1,
            ..Default::default()
        });

        // First request will fail with HttpDispatchError (can indicate a timeout).
        // Function should retry and succeed on second request.
        let e = upload_completed_part(&client, maybe_req, &completed_parts)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            true,
            predicate::str::contains("my timeout message").eval(&e)
        );
        assert_eq!(completed_parts.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_upload_completed_part_err_uploadpartrequest() {
        let _ = env_logger::try_init();

        let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default().with_body("blah"),
            MockCredentialsProvider,
            Default::default(),
        );
        let maybe_req = Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "it broke!",
        ));

        // First request will fail with HttpDispatchError (can indicate a timeout).
        // Function should retry and succeed on second request.
        let e = upload_completed_part(&client, maybe_req, &completed_parts)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(true, predicate::str::contains("it broke!").eval(&e));
        assert_eq!(completed_parts.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_upload_completed_part_chunk_err_exits_early() {
        let _ = env_logger::try_init();

        let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
        // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        let client = S3Client::new_with(
            MockRequestDispatcher::default().with_body("blah"),
            MockCredentialsProvider,
            Default::default(),
        );
        let maybe_req = Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "it broke!",
        ));

        // First request will fail with HttpDispatchError (can indicate a timeout).
        // Function should retry and succeed on second request.
        let e = upload_completed_part(&client, maybe_req, &completed_parts)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(true, predicate::str::contains("it broke!").eval(&e));
        assert_eq!(completed_parts.lock().unwrap().len(), 0);
    }
    // TODO: tests
    // maybe_req is err (is upload_completed_part the right spot to handle this? or should it receive req instead of maybe_req?)
    // test that chunk err should stop the unfold (switch to try_unfold)
    // response non-200

    // TODO: test that errors coming out of upload_completed_part actually error out of the upload process (stop all workers/tasks)

    // TODO: test create_multipart_upload failing with Credentials type RusotoError
    // https://docs.rs/rusoto_core/0.46.0/rusoto_core/enum.RusotoError.html

    // TODO: test if maybe_chunk is Err
}
