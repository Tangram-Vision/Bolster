// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use log::info;
use serde_json::json;

use super::error;
use super::hazard;
use super::models;

use super::api;
use crate::utils::app_config::AppConfig;

pub fn create_dataset() -> Result<()> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to sotrage provider

    // TODO: add context to error to say missing database jwt
    let jwt = AppConfig::get::<String>("database.jwt")?;
    let config = api::Configuration::new(jwt);
    let dataset = api::datasets::datasets_post(
        &config,
        json!({
            "metadata": {"description": "TODO: get from cmdline or prompt"},
            // TODO: remove url -- it will be moved to files table
            "url": "http://example.com",
        }),
    )?;
    // TODO: handle request error
    println!("{:?}", dataset);
    // TODO: make request
    // TODO: display output (new dataset's uuid)
    Ok(())
}

pub fn list_datasets() -> Result<Vec<models::Dataset>> {
    // TODO: at first, just create dataset
    // TODO: later, take optional list of files + upload them to sotrage provider

    // TODO: add context to error to say missing database jwt
    let jwt = AppConfig::get::<String>("database.jwt")?;
    let config = api::Configuration::new(jwt);
    let datasets = api::datasets::datasets_get(
        &config, None, None, None, None, None, None, None, None, None, None, None,
    )?;
    // TODO: handle request error
    println!("{:?}", datasets);
    // TODO: make request
    // TODO: display output (new dataset's uuid)
    Ok(datasets)
}

// TODO: add `ls` subcommand

/// Show the configuration file
pub fn hazard() -> Result<()> {
    // Generate, randomly, True or False
    let random_hazard: bool = hazard::generate_hazard()?;

    if random_hazard {
        println!("You got it right!");
    } else {
        println!("You got it wrong!");
    }

    Ok(())
}

/// Show the configuration file
pub fn config() -> Result<()> {
    let config = AppConfig::fetch()?;
    println!("{:#?}", config);

    Ok(())
}

/// Simulate an error
pub fn simulate_error() -> Result<()> {
    // Log this Error simulation
    info!("We are simulating an error");

    // Simulate an error
    error::simulate_error()?;

    // We should never get here...
    Ok(())
}

#[cfg(test)]
mod test {
    use super::create_dataset;
    use crate::utils::app_config::AppConfig;

    #[test]
    fn test_missing_database_jwt() {
        // Initialize configuration
        AppConfig::init(None).unwrap();
        let error = create_dataset().expect_err("Expected error due to missing database jwt");
        assert_eq!(
            error.to_string(),
            "configuration property \"database.jwt\" not found"
        );
    }
}
