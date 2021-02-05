// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use clap::{crate_authors, crate_description, crate_version};
use clap::{App, AppSettings, Arg};
use std::path::Path;
use std::str::FromStr;
use strum::VariantNames;
use uuid::Uuid;

use crate::app_config::{DatabaseConfig, StorageProviderChoices};
use crate::core::api;
use crate::core::api::storage;
use crate::core::commands;

/// Match commands
pub fn cli_match(config: config::Config, cli_matches: clap::ArgMatches) -> Result<()> {
    // Handle config subcommand first, because it doesn't need any valid configuration, and is helpful for debugging bad config!
    if let Some(("config", _config_matches)) = cli_matches.subcommand() {
        commands::config(config)?;
        return Ok(());
    }

    // Derive config needed for all commands (they all interact with the database)
    let jwt = config.clone().try_into::<DatabaseConfig>()?.database.jwt;
    let api_config = api::Configuration::new(jwt);

    // Handle all subcommands that interact with database or storage
    match cli_matches.subcommand() {
        Some(("create", _create_matches)) => {
            commands::create_dataset(&api_config)?;
        }
        Some(("ls", _ls_matches)) => {
            let datasets = commands::list_datasets(&api_config, None)?;

            // TODO: use generic, customizable formatter (e.g. kubernetes get)
            for d in datasets.iter() {
                println!("{} {} {}", d.uuid, d.created_date, d.url);
            }
        }
        Some(("upload", upload_matches)) => {
            // Safe to unwrap because arguments are required or have defaults
            let dataset_uuid: Uuid = upload_matches
                .value_of_t("dataset_uuid")
                .unwrap_or_else(|e| e.exit());
            let input_file = upload_matches.value_of("file").unwrap();
            let provider =
                StorageProviderChoices::from_str(upload_matches.value_of("provider").unwrap())?;
            let storage_config = storage::StorageConfig::new(config, provider)?;
            commands::update_dataset(
                &api_config,
                dataset_uuid,
                commands::upload_file(storage_config, dataset_uuid, Path::new(input_file))?,
            )?;
        }
        Some(("download", download_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_uuid: Uuid = download_matches
                .value_of_t("dataset_uuid")
                .unwrap_or_else(|e| e.exit());
            let datasets = commands::list_datasets(&api_config, Some(dataset_uuid))?;
            let dataset = &datasets[0];
            commands::download_file(config, &dataset.url)?;
        }
        _ => {
            // Arguments are required by default (in Clap).
            // This section should never execute.
            unreachable!("No matching subcommand!");
        }
    }
    Ok(())
}

/// Configure Clap
/// This function will configure clap and match arguments
pub fn cli_config() -> Result<clap::ArgMatches> {
    // Can't get default enum variant's &'static str, so own it here
    let default_storage_provider = StorageProviderChoices::default();

    let cli_app = App::new("bolster")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!("\n"))
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .about("Set a custom config file")
                .takes_value(true),
        )
        .subcommand(App::new("create").about("Create a new remote dataset"))
        .subcommand(App::new("ls").about("List remote datasets"))
        .subcommand(
            App::new("upload")
                .about("Upload file to remote dataset")
                .arg(Arg::new("dataset_uuid").required(true).takes_value(true))
                .arg(
                    Arg::new("file")
                        .required(true)
                        .value_name("FILE")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("provider")
                        .short('p')
                        .long("provider")
                        .value_name("PROVIDER")
                        .about("Upload to specified cloud storage provider")
                        .default_value(default_storage_provider.as_ref())
                        .possible_values(StorageProviderChoices::VARIANTS)
                        .takes_value(true),
                ),
        )
        .subcommand(
            App::new("download")
                .about("Download files in remote dataset")
                .arg(Arg::new("dataset_uuid").required(true).takes_value(true)),
            // TODO: add path to download files to?
        )
        .subcommand(App::new("config").about("Show Configuration"));

    // Get matches
    let cli_matches = cli_app.get_matches();

    Ok(cli_matches)
}
