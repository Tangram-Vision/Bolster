// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::Deserialize;
use std::path::PathBuf;
use std::vec::Vec;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Dataset {
    /// Note: This is a Primary Key.<pk/>
    pub dataset_id: Uuid,
    #[serde(with = "notz_rfc_3339")]
    pub created_date: DateTime<Utc>,
    /// File format, capture platform and OS, duration, number of streams, extrinsics/intrinsics, etc.
    /// Uses serde_json::Value type so it can represent arbitrary json as described at https://github.com/serde-rs/json/issues/144
    /// How does the user provide this metadata? Good question.
    pub metadata: serde_json::Value,
    pub files: Vec<UploadedFile>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DatasetNoFiles {
    /// Note: This is a Primary Key.<pk/>
    pub dataset_id: Uuid,
    #[serde(with = "notz_rfc_3339")]
    pub created_date: DateTime<Utc>,
    /// File format, capture platform and OS, duration, number of streams, extrinsics/intrinsics, etc.
    /// Uses serde_json::Value type so it can represent arbitrary json as described at https://github.com/serde-rs/json/issues/144
    /// How does the user provide this metadata? Good question.
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct UploadedFile {
    // Don't need to reference this value anywhere, so ignoring it
    // pub file_id: Uuid,
    pub dataset_id: Uuid,

    #[serde(with = "notz_rfc_3339")]
    pub created_date: DateTime<Utc>,
    pub url: Url,
    pub filesize: u64,
    // Likely unused, requesting the url w/o version downloads the latest version
    pub version: String,
    pub metadata: serde_json::Value,
}

impl UploadedFile {
    pub fn filepath_from_url(&self) -> Result<PathBuf> {
        let mut segments = self
            .url
            .path_segments()
            .ok_or_else(|| anyhow!("File URL is malformed!"))?;

        loop {
            if let Some(segment) = segments.next() {
                if segment == self.dataset_id.to_hyphenated().to_string() {
                    break;
                }
            } else {
                // We got to the end and never found the dataset id?
                bail!("File url ({}) doesn't contain dataset-id. Please contact support@tangramvision.com", self.url);
            }
        }
        Ok(segments.collect::<PathBuf>())
    }
}

// https://serde.rs/custom-date-format.html
mod notz_rfc_3339 {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer};
    // use serde::{self, Deserialize, Deserializer, Serializer};

    // Example: 2021-05-06T23:54:45.626411+00:00
    const FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f%:z";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    /*
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
    */

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use predicates::prelude::*;
    use serde_json::json;

    #[test]
    fn test_uploadedfile_filepath_from_url_success() {
        let dataset_id = Uuid::parse_str("d11cc371-f33b-4dad-ac2e-3c4cca30a256").unwrap();
        let url_str = format!(
            "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/{}/src/resources/test.dat",
            dataset_id
        );
        let uf = UploadedFile {
            dataset_id,
            created_date: Utc::now(),
            url: Url::parse(&url_str).unwrap(),
            filesize: 12,
            version: "blah".to_owned(),
            metadata: json!({}),
        };
        assert_eq!(
            "src/resources/test.dat",
            uf.filepath_from_url().unwrap().to_str().unwrap()
        );
    }

    #[test]
    fn test_uploadedfile_filepath_from_url_bad_url_missing_dataset_id() {
        let dataset_id = Uuid::parse_str("d11cc371-f33b-4dad-ac2e-3c4cca30a256").unwrap();
        let url_str = format!(
            "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/{}/src/resources/test.dat",
            "not-the-right-dataset-id"
        );
        let uf = UploadedFile {
            dataset_id,
            created_date: Utc::now(),
            url: Url::parse(&url_str).unwrap(),
            filesize: 12,
            version: "blah".to_owned(),
            metadata: json!({}),
        };
        let e = uf
            .filepath_from_url()
            .expect_err("Url doesn't contain the dataset-id")
            .to_string();
        assert_eq!(
            true,
            predicate::str::is_match(
                "File url .* doesn't contain dataset-id. Please contact support@tangramvision.com"
            )
            .unwrap()
            .eval(&e)
        );
    }
}
