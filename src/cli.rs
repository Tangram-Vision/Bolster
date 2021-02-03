// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use clap::{crate_authors, crate_description, crate_version};
use clap::{App, AppSettings, Arg};
use std::path::Path;
use uuid::Uuid;

use crate::app_config::AppConfig;
use crate::core::commands;

/// Match commands
pub fn cli_match() -> Result<()> {
    // Get matches
    let cli_matches = cli_config()?;

    // Merge clap config file if the value is set
    AppConfig::merge_config(cli_matches.value_of("config"))?;

    // Matches Commands or display help
    match cli_matches.subcommand() {
        Some(("create", _create_matches)) => {
            commands::create_dataset()?;
        }
        Some(("ls", _ls_matches)) => {
            commands::list_datasets()?;
        }
        Some(("upload", upload_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_uuid: Uuid = upload_matches
                .value_of_t("dataset_uuid")
                .unwrap_or_else(|e| e.exit());
            let input_file = upload_matches.value_of("file").unwrap();
            let url = commands::upload_file(dataset_uuid, Path::new(input_file))?;
            commands::update_dataset(dataset_uuid, url)?;
        }
        Some(("download", download_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_uuid: Uuid = download_matches
                .value_of_t("dataset_uuid")
                .unwrap_or_else(|e| e.exit());
            commands::download_file(dataset_uuid)?;
        }
        Some(("config", _config_matches)) => {
            commands::config()?;
        }
        _ => {
            // Arguments are required by default (in Clap)
            // This section should never execute and thus
            // should probably be logged in case it executed.
        }
    }
    Ok(())
}

/// Configure Clap
/// This function will configure clap and match arguments
pub fn cli_config() -> Result<clap::ArgMatches> {
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
