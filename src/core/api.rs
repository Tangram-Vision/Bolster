// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

pub mod datasets;
pub mod storage;

// TODO: Expose API functions we need to call from elsewhere
// pub use datasets::{datasets_create, etc...};

pub struct Configuration {
    pub base_path: String,
    pub user_agent: String,
    pub client: reqwest::blocking::Client,
    pub bearer_access_token: String,
}

impl Configuration {
    pub fn new(bearer_access_token: String) -> Configuration {
        let user_agent = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),);
        Configuration {
            base_path: "http://0.0.0.0:3000".to_owned(),
            client: reqwest::blocking::Client::new(),
            user_agent,
            bearer_access_token,
        }
    }
}
