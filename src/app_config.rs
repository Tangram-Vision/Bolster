// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use serde::Deserialize;
use strum_macros::{AsRefStr, EnumIter, EnumString, EnumVariantNames};

// TODO: should this be "UploadStorageProviderChoices"?
#[derive(AsRefStr, EnumVariantNames, EnumString, EnumIter)]
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
    pub fn from_url(url: &str) -> Result<StorageProviderChoices> {
        match url {
            x if x.contains(StorageProviderChoices::Aws.url_pattern()) => {
                Ok(StorageProviderChoices::Aws)
            }

            #[cfg(feature = "tangram-internal")]
            x if x.contains(StorageProviderChoices::DigitalOcean.url_pattern()) => {
                Ok(StorageProviderChoices::DigitalOcean)
            }

            _ => Err(anyhow!("Trying to download from unknown storage provider!")),
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
#[derive(Debug, Deserialize)]
pub struct CompleteAppConfig {
    pub database: Database,
    #[cfg(feature = "tangram-internal")]
    pub digitalocean_spaces: Option<StorageApiKeys>,
    pub aws_s3: Option<StorageApiKeys>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub database: Database,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct StorageApiKeys {
    pub access_key: String,
    pub secret_key: String,
}

#[cfg(test)]
mod tests {
    //use super::DatabaseConfig;
    // TODO: test full and partial configs

    /*
    #[test]
    fn fetch_full_config() {
        // Initialize configuration
        let config_contents = include_str!("resources/test_full_config.toml");
        AppConfig::init(Some(config_contents)).unwrap();

        // Fetch an instance of Config
        let config = AppConfig::fetch().unwrap();

        // Check the values
        assert_eq!(config.database.jwt, "abc");
        assert_eq!(
            config.digitalocean_spaces.as_ref().unwrap().access_key,
            "abc"
        );
        assert_eq!(
            config.digitalocean_spaces.as_ref().unwrap().secret_key,
            "def"
        );
        assert_eq!(config.aws_s3.as_ref().unwrap().access_key, "abc");
        assert_eq!(config.aws_s3.as_ref().unwrap().secret_key, "def");
    }

    #[test]
    fn fetch_partial_config() {
        // Initialize configuration
        let config_contents = include_str!("resources/test_partial_config.toml");
        AppConfig::init(Some(config_contents)).unwrap();

        // Fetch an instance of Config
        let config = AppConfig::fetch().unwrap();

        // Check the values
        assert_eq!(config.database.jwt, "abc");
        assert!(config.digitalocean_spaces.as_ref().is_none());
        assert_eq!(config.aws_s3.as_ref().unwrap().access_key, "abc");
        assert_eq!(config.aws_s3.as_ref().unwrap().secret_key, "def");
    }

    // Can't test overriding with environment variables because they're set at
    // the process level and mess up other tests.
    #[test]
    fn env_var_override() {
        // Initialize configuration
        let config_contents = include_str!("../resources/test_partial_config.toml");
        println!("env_var_override {}", config_contents);
        env::set_var("BOLSTER_AWS_S3__SECRET_KEY", "so secret");
        AppConfig::init(Some(config_contents)).unwrap();

        // Fetch an instance of Config
        let config = AppConfig::fetch().unwrap();

        // Check the values
        assert_eq!(config.database.as_ref().unwrap().jwt, "abc");
        assert!(config.digitalocean_spaces.as_ref().is_none());
        assert_eq!(config.aws_s3.as_ref().unwrap().access_key, "abc");
        assert_eq!(config.aws_s3.as_ref().unwrap().secret_key, "so secret");
        env::remove_var("BOLSTER_AWS_S3__SECRET_KEY");
    }

    #[test]
    fn verify_get() {
        // Initialize configuration
        let config_contents = include_str!("resources/test_full_config.toml");
        AppConfig::init(Some(config_contents)).as_ref().unwrap();

        // Check value with get
        assert_eq!(AppConfig::get::<String>("database.jwt").unwrap(), "abc");
    }

    #[test]
    fn verify_set() {
        // Initialize configuration
        let config_contents = include_str!("resources/test_full_config.toml");
        AppConfig::init(Some(config_contents)).unwrap();

        // Set a field
        AppConfig::set("database.jwt", "new jwt").unwrap();

        // Fetch a new instance of Config
        let config = AppConfig::fetch().unwrap();

        // Check value was modified
        assert_eq!(config.database.jwt, "new jwt");
    }
    */
}
