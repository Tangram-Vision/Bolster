// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::{anyhow, Result};
use rusoto_core::{request, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use std::convert::TryFrom;
use tokio::fs::File;
use tokio::io;

use crate::app_config::AppConfig;

pub struct StorageConfig {
    credentials: StaticProvider,
    bucket: String,
    region: Region,
}

pub mod digitalocean_spaces {
    use super::StorageConfig;
    use rusoto_core::Region;
    use rusoto_credential::StaticProvider;

    pub fn new_config(access_key: String, secret_key: String) -> StorageConfig {
        StorageConfig {
            credentials: StaticProvider::new_minimal(access_key, secret_key),
            bucket: String::from("tangs-stage"),
            region: Region::Custom {
                name: "sfo2".to_owned(),
                endpoint: "sfo2.digitaloceanspaces.com".to_owned(),
            },
        }
    }
}

pub mod aws_s3 {
    use super::StorageConfig;
    use rusoto_core::Region;
    use rusoto_credential::StaticProvider;

    pub fn new_config(access_key: String, secret_key: String) -> StorageConfig {
        StorageConfig {
            credentials: StaticProvider::new_minimal(access_key, secret_key),
            bucket: String::from("tangram-datasets"),
            region: Region::UsEast2,
        }
    }
}

impl TryFrom<AppConfig> for StorageConfig {
    type Error = anyhow::Error;

    fn try_from(config: AppConfig) -> Result<Self> {
        if let Some(do_config) = config.digitalocean_spaces {
            return Ok(digitalocean_spaces::new_config(
                do_config.access_key,
                do_config.secret_key,
            ));
        };
        if let Some(s3_config) = config.aws_s3 {
            return Ok(aws_s3::new_config(
                s3_config.access_key,
                s3_config.secret_key,
            ));
        };
        Err(anyhow!("Missing AWS S3 config"))
    }
}

#[tokio::main]
pub async fn upload_file(data: Vec<u8>, key: String, config: StorageConfig) -> Result<String> {
    let region_endpoint = match &config.region {
        Region::Custom { endpoint, .. } => endpoint.clone(),
        r => format!("s3.{}.amazonaws.com", r.name()),
    };
    // Constructing url here to avoid borrow errors if we try to construct it at
    // the bottom of the function
    let url = format!("https://{}.{}/{}", config.bucket, region_endpoint, key);

    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);
    let req = PutObjectRequest {
        // bucket: "tangs-stage".to_owned(),
        bucket: config.bucket,
        // TODO: use actual file
        // TODO: how to build key?
        body: Some(data.into()),
        key,
        ..Default::default()
    };
    // just spawn tokio here and use it, instead of async-ing everything yet
    // TODO: use example https://github.com/softprops/elblogs/blob/96df314db92216a769dc92d90a5cb0ae42bb13da/src/main.rs#L212-L223
    // TODO: another reference https://stackoverflow.com/questions/57810173/streamed-upload-to-s3-with-rusoto

    // https://www.rusoto.org/futures.html mentions turning futures into blocking calls
    let resp = client.put_object(req).await?;
    println!("response {:?}", resp);
    // TODO: get version_id and store to database
    Ok(url)
}

// TODO: Use reqwest Url type
#[tokio::main]
pub async fn download_file(url: &str) -> Result<()> {
    // TODO: Is there a better way to do this, like how try_from works for getting upload config?
    let app_config = AppConfig::fetch()?;
    let config;
    if url.contains("amazonaws.com") {
        let s3_config = app_config
            .digitalocean_spaces
            .ok_or_else(|| anyhow!("Missing DigitalOcean API keys to download: {}", url))?;
        config = aws_s3::new_config(s3_config.access_key, s3_config.secret_key);
    } else if url.contains("digitaloceanspaces.com") {
        let do_config = app_config
            .digitalocean_spaces
            .ok_or_else(|| anyhow!("Missing DigitalOcean API keys to download: {}", url))?;
        config = digitalocean_spaces::new_config(do_config.access_key, do_config.secret_key);
    } else {
        return Err(anyhow!("Trying to download from unknown storage provider!"));
    }

    // TODO: store provider, bucket, and key separately in database?
    let key = url
        .replacen("https://", "", 1)
        .split('/')
        .nth(1)
        .ok_or_else(|| anyhow!("Unexpected url format: {}", url))?
        .to_string();

    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);
    let req = GetObjectRequest {
        bucket: config.bucket,
        key: key.to_owned(),
        ..Default::default()
    };

    let resp = client.get_object(req).await?;
    println!("response {:?}", resp);
    let body = resp.body.ok_or_else(|| anyhow!("Empty file! {}", url))?;
    let mut body = body.into_async_read();
    let mut file = File::create("outputfile").await?;
    io::copy(&mut body, &mut file).await?;
    Ok(())
}
