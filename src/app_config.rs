// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use config::{Config, Environment};
use lazy_static::lazy_static;
use serde::Deserialize;
use std::ops::Deref;
use std::sync::RwLock;

// CONFIG static variable. It's actually an AppConfig
// inside an RwLock.
lazy_static! {
    static ref CONFIG: RwLock<Config> = RwLock::new(Config::new());
}

#[derive(Debug, Deserialize)]
pub struct Database {
    pub jwt: String,
}

#[derive(Debug, Deserialize)]
pub struct DigitalOceanSpaces {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
pub struct AwsS3 {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database: Database,
    pub digitalocean_spaces: Option<DigitalOceanSpaces>,
    pub aws_s3: Option<AwsS3>,
}

impl AppConfig {
    pub fn init(user_config: Option<&str>) -> Result<()> {
        let mut settings = Config::new();

        // Merge config file (default config in regular operation)
        if let Some(config_contents) = user_config {
            //let contents = include_str!(config_file_path);
            settings.merge(config::File::from_str(
                &config_contents,
                config::FileFormat::Toml,
            ))?;
        }

        // Merge settings with env variables
        // Separator allows reaching into structs (e.g. AWS_S3__ACCESS_KEY=foo)
        settings.merge(Environment::with_prefix("BOLSTER").separator("__"))?;

        // TODO: Merge settings with Clap Settings Arguments

        // Save Config to RwLoc
        {
            let mut w = CONFIG.write().unwrap();
            *w = settings;
        }

        Ok(())
    }

    pub fn merge_config(config_file: Option<&str>) -> Result<()> {
        // Merge settings with config file if there is one
        if let Some(config_file_path) = config_file {
            {
                CONFIG
                    .write()
                    .unwrap()
                    .merge(config::File::with_name(config_file_path))?;
            }
        }
        Ok(())
    }

    // Set CONFIG
    pub fn set(key: &str, value: &str) -> Result<()> {
        {
            // Set Property
            CONFIG.write().unwrap().set(key, value)?;
        }

        Ok(())
    }

    // Get a single value
    pub fn get<'de, T>(key: &'de str) -> Result<T>
    where
        T: serde::Deserialize<'de>,
    {
        Ok(CONFIG.read().unwrap().get::<T>(key)?)
    }

    // Get CONFIG
    // This clones Config (from RwLock<Config>) into a new AppConfig object.
    // This means you have to fetch this again if you changed the configuration.
    pub fn fetch() -> Result<AppConfig> {
        // Get a Read Lock from RwLock
        let r = CONFIG.read().unwrap();

        // Clone the Config object
        let config_clone = r.deref().clone();

        // Coerce Config into AppConfig
        Ok(config_clone.try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

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

    /*
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
    */

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
}
