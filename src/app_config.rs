// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

//! Structs and helper methods for using data in the bolster config file.

use std::cmp::PartialEq;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumIter, EnumString, EnumVariantNames};
use uuid::Uuid;

/// Available choices of cloud storage providers.
///
/// To use a cloud storage provider, valid credentials must be present in the
/// bolster config file.
#[derive(AsRefStr, EnumVariantNames, EnumString, EnumIter, Debug, PartialEq)]
pub enum StorageProviderChoices {
    /// DigitalOcean Spaces
    #[cfg(feature = "tangram-internal")]
    #[strum(serialize = "digitalocean")]
    DigitalOcean,
    /// AWS S3
    #[strum(serialize = "aws")]
    Aws,
}

impl StorageProviderChoices {
    /// The domain name corresponding to the storage provider.
    pub fn url_pattern(&self) -> &'static str {
        match *self {
            #[cfg(feature = "tangram-internal")]
            StorageProviderChoices::DigitalOcean => "digitaloceanspaces.com",
            StorageProviderChoices::Aws => "amazonaws.com",
        }
    }
    /// Derives the storage provider enum value from a url.
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
#[derive(Debug, Deserialize, Serialize)]
pub struct CompleteAppConfig {
    pub database: Database,
    #[cfg(feature = "tangram-internal")]
    pub digitalocean_spaces: Option<StorageApiKeys>,
    pub aws_s3: Option<StorageApiKeys>,
}

/// Container for configuration values for connecting + authenticating with the
/// datasets database.
#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub database: Database,
}

/// Database connection and authentication details.
#[derive(Debug, Deserialize, Serialize)]
pub struct Database {
    pub jwt: String,
    pub url: Url,
}

/// Container for configuration values for connecting to DigitalOcean Spaces
/// cloud storage.
#[cfg(feature = "tangram-internal")]
#[derive(Debug, Deserialize)]
pub struct DigitalOceanSpacesConfig {
    pub digitalocean_spaces: StorageApiKeys,
}

/// Container for configuration values for connecting to AWS S3 cloud storage.
#[derive(Debug, Deserialize)]
pub struct AwsS3Config {
    pub aws_s3: StorageApiKeys,
}

/// Auth keys for S3-compatible cloud storage providers.
#[derive(Debug, Deserialize, Serialize)]
pub struct StorageApiKeys {
    pub access_key: String,
    pub secret_key: String,
}

impl Database {
    /// Extracts the user id (a [Uuid]) from the database JWT.
    ///
    /// # Examples
    ///
    /// Example is ignored because no bolster modules are public. Update this
    /// doctest if modules are changed to be public.
    ///
    /// ```ignore
    /// # use std::str::FromStr;
    /// # use bolster::app_config::Database;
    /// let db = Database {
    ///     url: reqwest::Url::from_str("http://example.com").unwrap(),
    ///     jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lk\
    ///                        IjoiZjYwYTg0M2EtMjVhYy00YzU0LWExNjktNWU5MDk3YjY5Z\
    ///                        jQzIiwicm9sZSI6IndlYl91c2VyIiwiaWF0IjoxNjIwODQ3Nj\
    ///                        Q4fQ.NE3gOa2dg7xh1hRpr0haDWLLOxqmK8BBvmD-rQfYpuQ"),
    /// };
    /// assert_eq!(
    ///     uuid::Uuid::parse_str("f60a843a-25ac-4c54-a169-5e9097b69f43").unwrap(),
    ///     db.user_id_from_jwt().unwrap()
    /// );
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the database JWT is malformed (not made up of 3
    /// parts, not base64-encoded, not valid UTF-8, doesn't contain valid json,
    /// is missing a required field, or if the data in the JWT is malformed).
    pub fn user_id_from_jwt(self) -> Result<Uuid> {
        let jwt_parts: Vec<&str> = self.jwt.split('.').collect();
        if jwt_parts.len() != 3 {
            bail!("Config error: Database jwt is malformed (expected 3 period-delimited segments)");
        }
        let jwt_payload: &str = jwt_parts[1];
        let bytes = base64::decode(jwt_payload)
            .context("Config error: Database jwt is malformed (expected base64 encoding)")?;
        let jwt_str =
            String::from_utf8(bytes).context("Config error: Database jwt isn't valid UTF-8")?;
        let parsed: serde_json::Value = serde_json::from_str(&jwt_str)
            .context("Config error: Database jwt doesn't contain valid JSON")?;
        let user_id = parsed["user_id"]
            .as_str()
            .context("Config error: Database jwt doesn't contain expected field: user_id")?;
        let user_uuid: Uuid = Uuid::parse_str(user_id)
            .context("Config error: Database jwt's user_id isn't a valid UUID")?;
        Ok(user_uuid)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use predicates::prelude::*;

    use super::*;

    #[test]
    fn test_bad_url_to_provider_enum() {
        let error = StorageProviderChoices::from_url(&Url::from_str("http://example.com").unwrap())
            .expect_err("Url shouldn't be recognized as a storage provider url");
        assert!(
            error
                .to_string()
                .contains("Trying to download from unknown storage provider:"),
            "{}",
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
            "{}",
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
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn test_jwt_decode() {
        let jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZjYwYTg0M2EtMjVhYy00YzU0LWExNjktNWU5MDk3YjY5ZjQzIiwicm9sZSI6IndlYl91c2VyIiwiaWF0IjoxNjIwODQ3NjQ4fQ.NE3gOa2dg7xh1hRpr0haDWLLOxqmK8BBvmD-rQfYpuQ";
        let jwt_payload: &str = jwt.split('.').collect::<Vec<&str>>()[1];
        let bytes = base64::decode(jwt_payload).expect("JWT payload didn't decode");
        let jwt_str = String::from_utf8(bytes).expect("Invalid UTF-8");
        println!("{}", jwt_str);
        let parsed: serde_json::Value = serde_json::from_str(&jwt_str).expect("Bad json");
        println!("user_id is {}", parsed["user_id"]);
        let user_uuid: Uuid =
            Uuid::parse_str(parsed["user_id"].as_str().expect("string user id")).expect("Bad uuid");
        println!("uuid is {}", user_uuid);
    }

    #[test]
    fn test_user_id_from_jwt_success() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZjYwYTg0M2EtMjVhYy00YzU0LWExNjktNWU5MDk3YjY5ZjQzIiwicm9sZSI6IndlYl91c2VyIiwiaWF0IjoxNjIwODQ3NjQ4fQ.NE3gOa2dg7xh1hRpr0haDWLLOxqmK8BBvmD-rQfYpuQ"),
        };
        assert_eq!(
            Uuid::parse_str("f60a843a-25ac-4c54-a169-5e9097b69f43").unwrap(),
            db.user_id_from_jwt().unwrap()
        );
    }

    #[test]
    fn test_user_id_from_jwt_malformed_jwt() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZjYwYTg0M2EtMjVhYy00YzU0LWExNjktNWU5MDk3YjY5ZjQzIiwicm9sZSI6IndlYl91c2VyIiwiaWF0IjoxNjIwODQ3NjQ4fQ"),
        };
        assert_eq!(
            true,
            predicate::str::contains("expected 3 period-delimited segments")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_bad_encoding() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("not.base64.encoded"),
        };
        assert_eq!(
            true,
            predicate::str::contains("expected base64 encoding")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_not_utf8() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("//5iAGwAYQBoAA==.//5iAGwAYQBoAA==.//5iAGwAYQBoAA=="),
        };
        assert_eq!(
            true,
            predicate::str::contains("isn't valid UTF-8")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_not_json() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("YmxhaA==.YmxhaA==.YmxhaA=="),
        };
        assert_eq!(
            true,
            predicate::str::contains("doesn't contain valid JSON")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_missing_user_id() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJibGFoIjoiYmxhaCJ9.5Oi7vKR1ur19mUy8UH_QALnKXCdWuWP9MiPCXbPb49g"),
        };
        assert_eq!(
            true,
            predicate::str::contains("doesn't contain expected field: user_id")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_user_id_not_uuid() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYmxhaCJ9.SLDLrwQwp3a6GNga05HFipYnMpsWizwzBpfp78wTaHg"),
        };
        assert_eq!(
            true,
            predicate::str::contains("user_id isn't a valid UUID")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }
}

#[cfg(all(test, feature = "tangram-internal"))]
mod tests_internal {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_digitalocean_provider_available() {
        let val = StorageProviderChoices::from_url(
            &Url::from_str("https://digitaloceanspaces.com/bucket/key").unwrap(),
        )
        .expect("Url should be recognized");
        assert_eq!(val, StorageProviderChoices::DigitalOcean);
    }
}
