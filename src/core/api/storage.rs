// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::{anyhow, Result};
use log::debug;
use reqwest::Url;
use rusoto_core::{request, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use tokio::fs::File;
use tokio::io;

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
                    bucket: String::from("tangram-datasets"),
                    region: Region::UsEast2,
                })
            }
        }
    }
}

#[tokio::main]
pub async fn upload_file(
    config: StorageConfig,
    data: Vec<u8>,
    key: String,
) -> Result<(Url, String)> {
    let region_endpoint = match &config.region {
        Region::Custom { endpoint, .. } => endpoint.clone(),
        r => format!("s3.{}.amazonaws.com", r.name()),
    };
    // Constructing url here to avoid borrow errors if we try to construct it at
    // the bottom of the function
    // TODO: Use reqwest Url type
    let url_str = format!("https://{}.{}/{}", config.bucket, region_endpoint, key);
    let url = Url::parse(&url_str)?;

    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);
    let req = PutObjectRequest {
        bucket: config.bucket,
        body: Some(data.into()),
        key,
        ..Default::default()
    };
    debug!("making upload_file request {:?}", req);
    // just spawn tokio here and use it, instead of async-ing everything yet
    // TODO: use example https://github.com/softprops/elblogs/blob/96df314db92216a769dc92d90a5cb0ae42bb13da/src/main.rs#L212-L223
    // TODO: another reference https://stackoverflow.com/questions/57810173/streamed-upload-to-s3-with-rusoto

    // https://www.rusoto.org/futures.html mentions turning futures into blocking calls
    let resp = client.put_object(req).await?;
    debug!("upload_file response {:?}", resp);
    let version = resp
        .version_id
        .ok_or_else(|| anyhow!("Uploaded file wasn't versioned by storage provider"))?;
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

    let dispatcher = request::HttpClient::new().unwrap();
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
}
