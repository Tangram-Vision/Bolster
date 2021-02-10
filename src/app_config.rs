// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::cmp;
use strum_macros::{AsRefStr, EnumIter, EnumString, EnumVariantNames};

// TODO: should this be "UploadStorageProviderChoices"?
#[derive(AsRefStr, EnumVariantNames, EnumString, EnumIter, Debug, cmp::PartialEq)]
pub enum StorageProviderChoices {
    #[cfg(feature = "tangram-internal")]
    #[strum(serialize = "digitalocean")]
    DigitalOcean,
    #[strum(serialize = "aws")]
    Aws,
}

impl StorageProviderChoices {
    pub fn url_pattern(&self) -> &'static str {
        match *self {
            #[cfg(feature = "tangram-internal")]
            StorageProviderChoices::DigitalOcean => "digitaloceanspaces.com",
            StorageProviderChoices::Aws => "amazonaws.com",
        }
    }
    pub fn from_url(url: &Url) -> Result<StorageProviderChoices> {
        match url
            .domain()
            .ok_or_else(|| anyhow!("Storage provider url doesn't contain a domain: {}", url))?
        {
            x if x.contains(StorageProviderChoices::Aws.url_pattern()) => {
                Ok(StorageProviderChoices::Aws)
            }

            #[cfg(feature = "tangram-internal")]
            x if x.contains(StorageProviderChoices::DigitalOcean.url_pattern()) => {
                Ok(StorageProviderChoices::DigitalOcean)
            }

            _ => Err(anyhow!(
                "Trying to download from unknown storage provider: {}",
                url
            )),
        }
    }
}

impl Default for StorageProviderChoices {
    #[cfg(feature = "tangram-internal")]
    fn default() -> Self {
        StorageProviderChoices::DigitalOcean
    }
    #[cfg(not(feature = "tangram-internal"))]
    fn default() -> Self {
        StorageProviderChoices::Aws
    }
}

/// Used only for `config` subcommand to show all config.
/// When interacting with the database, the DatabaseConfig below is used. When
/// the code knows which storage provider to use for upload/download, it
/// deserializes the config with DigitalOceanSpacesConfig or AwsS3Config, as
/// appropriate.
#[derive(Debug, Deserialize, Serialize)]
pub struct CompleteAppConfig {
    pub database: Database,
    #[cfg(feature = "tangram-internal")]
    pub digitalocean_spaces: Option<StorageApiKeys>,
    pub aws_s3: Option<StorageApiKeys>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub database: Database,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Database {
    pub jwt: String,
}

#[cfg(feature = "tangram-internal")]
#[derive(Debug, Deserialize)]
pub struct DigitalOceanSpacesConfig {
    pub digitalocean_spaces: StorageApiKeys,
}

#[derive(Debug, Deserialize)]
pub struct AwsS3Config {
    pub aws_s3: StorageApiKeys,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StorageApiKeys {
    pub access_key: String,
    pub secret_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_bad_url_to_provider_enum() {
        let error = StorageProviderChoices::from_url(&Url::from_str("http://example.com").unwrap())
            .expect_err("Url shouldn't be recognized as a storage provider url");
        assert!(
            error
                .to_string()
                .contains("Trying to download from unknown storage provider:"),
            error.to_string()
        );
    }

    #[test]
    fn test_ip_addr_url_to_provider_enum() {
        let error = StorageProviderChoices::from_url(&Url::from_str("http://127.0.0.1").unwrap())
            .expect_err("Url shouldn't be recognized as a storage provider url");
        assert!(
            error
                .to_string()
                .contains("Storage provider url doesn't contain a domain:"),
            error.to_string()
        );
    }

    #[test]
    fn test_digitalocean_provider_unavailable() {
        let error = StorageProviderChoices::from_url(
            &Url::from_str("https://digitaloceanspaces.com/bucket/key").unwrap(),
        )
        .expect_err("Url shouldn't be recognized as a storage provider url");
        assert!(
            error
                .to_string()
                .contains("Trying to download from unknown storage provider:"),
            error.to_string()
        );
    }
}

#[cfg(all(test, feature = "tangram-internal"))]
mod tests_internal {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_digitalocean_provider_available() {
        let val = StorageProviderChoices::from_url(
            &Url::from_str("https://digitaloceanspaces.com/bucket/key").unwrap(),
        )
        .expect("Url should be recognized");
        assert_eq!(val, StorageProviderChoices::DigitalOcean);
    }
}
