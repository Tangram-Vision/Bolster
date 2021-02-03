// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

// TODO: extract common code between aws/digitalocean

use anyhow::Result;
use rusoto_core::{request, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

#[tokio::main]
pub async fn upload_file(
    data: Vec<u8>,
    bucket: &str,
    key: &str,
    region: Region,
    credentials: StaticProvider,
) -> Result<()> {
    let dispatcher = request::HttpClient::new().unwrap();
    // credential docs: https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let client = S3Client::new_with(dispatcher, credentials, region);
    let req = PutObjectRequest {
        // bucket: "tangs-stage".to_owned(),
        bucket: bucket.to_owned(),
        // TODO: use actual file
        // TODO: how to build key?
        body: Some(data.into()),
        key: key.to_owned(),
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
