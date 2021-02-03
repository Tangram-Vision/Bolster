// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dataset {
    /// Note: This is a Primary Key.<pk/>
    #[serde(rename = "uuid")]
    pub uuid: String,
    #[serde(rename = "created_date", with = "notz_rfc_3339")]
    pub created_date: DateTime<Utc>,
    #[serde(rename = "creator_role")]
    pub creator_role: String,
    #[serde(rename = "access_role")]
    pub access_role: String,
    #[serde(rename = "url")]
    pub url: String,
    /// File format, capture platform and OS, duration, number of streams, extrinsics/intrinsics, etc.
    /// Uses serde_json::Value type so it can represent arbitrary json as described at https://github.com/serde-rs/json/issues/144
    /// How does the user provide this metadata? Good question.
    #[serde(rename = "metadata")]
    pub metadata: serde_json::Value,
}

/*
impl Dataset {
    pub fn new(
        uuid: String,
        created_date: DateTime<Utc>,
        creator_role: String,
        access_role: String,
        url: String,
        metadata: serde_json::Value,
    ) -> Dataset {
        Dataset {
            uuid,
            created_date,
            creator_role,
            access_role,
            url,
            metadata,
        }
    }
}
*/

// https://serde.rs/custom-date-format.html
mod notz_rfc_3339 {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

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
