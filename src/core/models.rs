// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use serde::{Deserialize, Serialize};

// TODO: fix types in dataset mode
// TODO: fix types in dataset mode
// TODO: fix types in dataset mode
// TODO: fix types in dataset mode
// TODO: fix types in dataset mode
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dataset {
    /// Note: This is a Primary Key.<pk/>
    #[serde(rename = "uuid")]
    pub uuid: String,
    #[serde(rename = "created_date")]
    pub created_date: String,
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

impl Dataset {
    pub fn new(
        uuid: String,
        created_date: String,
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
