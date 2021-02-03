// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::{anyhow, Result};
use rusoto_core::{request, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::convert::TryFrom;

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
pub async fn upload_file(data: Vec<u8>, key: String, config: StorageConfig) -> Result<()> {
    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, config.credentials, config.region);
    let req = PutObjectRequest {
        // bucket: "tangs-stage".to_owned(),
        bucket: config.bucket,
        // TODO: use actual file
        // TODO: how to build key?
        body: Some(data.into()),
        key: key,
        ..Default::default()
    };
    // just spawn tokio here and use it, instead of async-ing everything yet
    // TODO: use example https://github.com/softprops/elblogs/blob/96df314db92216a769dc92d90a5cb0ae42bb13da/src/main.rs#L212-L223
    // TODO: another reference https://stackoverflow.com/questions/57810173/streamed-upload-to-s3-with-rusoto

    // https://www.rusoto.org/futures.html mentions turning futures into blocking calls
    let resp = client.put_object(req).await?;
    println!("response {:?}", resp);
    // TODO: get version_id and store to database
    Ok(())
}
