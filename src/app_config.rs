//! Structs and helper methods for using data in the bolster config file.

use anyhow::{bail, Context, Result};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Used only for `config` subcommand to show all config.
#[derive(Debug, Deserialize, Serialize)]
pub struct CompleteAppConfig {
    /// Database connection and authentication details.
    pub database: Database,
}

/// Container for configuration values for connecting + authenticating with the
/// datasets database.
#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    /// Database connection and authentication details.
    pub database: Database,
}

/// Database connection and authentication details.
#[derive(Debug, Deserialize, Serialize)]
pub struct Database {
    /// Authentication token
    pub jwt: String,
    /// Database endpoint
    pub url: Url,
    /// Storage bucket (group id)
    pub bucket: String,
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
    ///     bucket: "test",
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
        let user_id = parsed["sub"]
            .as_str()
            .context("Config error: Database jwt doesn't contain expected field: sub")?;
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
            bucket: "test".to_owned(),
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
            bucket: "test".to_owned(),
        };
        assert!(
            predicate::str::contains("expected 3 period-delimited segments")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_bad_encoding() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("not.base64.encoded"),
            bucket: "test".to_owned(),
        };
        assert!(predicate::str::contains("expected base64 encoding")
            .eval(&db.user_id_from_jwt().unwrap_err().to_string()));
    }

    #[test]
    fn test_user_id_from_jwt_not_utf8() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("//5iAGwAYQBoAA==.//5iAGwAYQBoAA==.//5iAGwAYQBoAA=="),
            bucket: "test".to_owned(),
        };
        assert!(predicate::str::contains("isn't valid UTF-8")
            .eval(&db.user_id_from_jwt().unwrap_err().to_string()));
    }

    #[test]
    fn test_user_id_from_jwt_not_json() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("YmxhaA==.YmxhaA==.YmxhaA=="),
            bucket: "test".to_owned(),
        };
        assert!(predicate::str::contains("doesn't contain valid JSON")
            .eval(&db.user_id_from_jwt().unwrap_err().to_string()));
    }

    #[test]
    fn test_user_id_from_jwt_missing_user_id() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJibGFoIjoiYmxhaCJ9.5Oi7vKR1ur19mUy8UH_QALnKXCdWuWP9MiPCXbPb49g"),
            bucket: "test".to_owned(),
        };
        assert!(
            predicate::str::contains("doesn't contain expected field: user_id")
                .eval(&db.user_id_from_jwt().unwrap_err().to_string())
        );
    }

    #[test]
    fn test_user_id_from_jwt_user_id_not_uuid() {
        let db = Database {
            url: Url::from_str("http://example.com").unwrap(),
            jwt: String::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYmxhaCJ9.SLDLrwQwp3a6GNga05HFipYnMpsWizwzBpfp78wTaHg"),
            bucket: "test".to_owned(),
        };
        assert!(predicate::str::contains("user_id isn't a valid UUID")
            .eval(&db.user_id_from_jwt().unwrap_err().to_string()));
    }
}
