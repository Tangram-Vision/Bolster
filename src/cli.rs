// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use chrono::NaiveDate;
use clap::{crate_authors, crate_description, crate_version};
use clap::{App, AppSettings, Arg};
use std::path::Path;
use std::str::FromStr;
use strum::VariantNames;
use uuid::Uuid;

use crate::app_config::{DatabaseConfig, StorageProviderChoices};
use crate::core::api::datasets::{DatabaseAPIConfig, DatasetGetRequest, DatasetOrdering};
use crate::core::api::storage;
use crate::core::commands;

/// Extract optional arg with a specific type, exiting on parse error
pub fn handle_optional_arg<T>(matches: &clap::ArgMatches, arg_name: &str) -> Option<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    match matches.value_of_t(arg_name) {
        Ok(val) => Some(val),
        Err(clap::Error {
            kind: clap::ErrorKind::ArgumentNotFound,
            ..
        }) => None,
        Err(e) => e.exit(),
    }
}

/// Match commands
pub fn cli_match(config: config::Config, cli_matches: clap::ArgMatches) -> Result<()> {
    // Handle config subcommand first, because it doesn't need any valid configuration, and is helpful for debugging bad config!
    if let Some(("config", _config_matches)) = cli_matches.subcommand() {
        commands::print_config(config)?;
        return Ok(());
    }

    // Derive config needed for all commands (they all interact with the database)
    let jwt = config.clone().try_into::<DatabaseConfig>()?.database.jwt;
    let db_config = DatabaseAPIConfig::new(jwt)?;

    // Handle all subcommands that interact with database or storage
    match cli_matches.subcommand() {
        Some(("create", _create_matches)) => {
            commands::create_dataset(&db_config)?;
        }
        Some(("ls", ls_matches)) => {
            // For optional arguments, if they're missing (ArgumentNotFound)
            // treat it as Option::None. Any other error should cause an exit
            // and error message.
            let after_date: Option<NaiveDate> = handle_optional_arg(ls_matches, "after_date");
            let before_date: Option<NaiveDate> = handle_optional_arg(ls_matches, "before_date");

            // Validation to ensure before and after date bounds are sane
            if let (Some(before), Some(after)) = (before_date, after_date) {
                if before < after {
                    clap::Error::with_description(
                        format!(
                            "before_date ({}) must be later than the after_date ({})",
                            before, after
                        ),
                        clap::ErrorKind::ValueValidation,
                    )
                    .exit();
                }
            }

            let creator: Option<String> = handle_optional_arg(ls_matches, "creator");

            // TODO: implement metadata CLI input

            let uuid: Option<Uuid> = handle_optional_arg(ls_matches, "uuid");
            let limit: Option<usize> = handle_optional_arg(ls_matches, "limit");
            let offset: Option<usize> = handle_optional_arg(ls_matches, "offset");

            // TODO: implement order
            let order: Option<DatasetOrdering> = handle_optional_arg(ls_matches, "order");

            let get_params = DatasetGetRequest {
                uuid,
                before_date,
                after_date,
                creator,
                order,
                limit,
                offset,
            };

            let datasets = commands::list_datasets(&db_config, &get_params)?;

            // TODO: use generic, customizable formatter (e.g. kubernetes get)
            // TODO: show creator for tangram-internal build
            for d in datasets.iter() {
                println!("{} {} {}", d.uuid, d.created_date, d.url);
            }
        }
        Some(("upload", upload_matches)) => {
            // Safe to unwrap because arguments are required or have defaults
            let dataset_uuid: Uuid = upload_matches.value_of_t_or_exit("dataset_uuid");
            let input_file = upload_matches.value_of("file").unwrap();
            let provider =
                StorageProviderChoices::from_str(upload_matches.value_of("provider").unwrap())?;
            let storage_config = storage::StorageConfig::new(config, provider)?;
            let url = commands::upload_file(storage_config, dataset_uuid, Path::new(input_file))?;
            commands::update_dataset(&db_config, dataset_uuid, &url)?;
        }
        Some(("download", download_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_uuid: Uuid = download_matches.value_of_t_or_exit("dataset_uuid");
            let get_params = DatasetGetRequest {
                uuid: Some(dataset_uuid),
                ..Default::default()
            };
            let datasets = commands::list_datasets(&db_config, &get_params)?;
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
        .subcommand(
            App::new("ls")
                .about("List remote datasets")
                // Using `.args` instead of repeated `.arg` so we can apply a feature flag
                .args(&[
                    Arg::new("after_date")
                        .about("Show datasets created on or after 00:00 UTC of this date (format: YYYY-mm-dd)")
                        .short('a')
                        .long("after-date")
                        .value_name("DATE")
                        .takes_value(true),
                    Arg::new("before_date")
                        .about("Show datasets created before 00:00 UTC of this date (format: YYYY-mm-dd)")
                        .short('b')
                        .long("before-date")
                        .value_name("DATE")
                        .takes_value(true),
                    #[cfg(feature = "tangram-internal")]
                    Arg::new("creator")
                        .about("Show datasets created by this user")
                        .short('c')
                        .long("creator")
                        .value_name("USERNAME")
                        .takes_value(true),
                    // TODO: implement metadata
                    Arg::new("metadata")
                        .about("NOT IMPLEMENTED: Show dataset matching metadata")
                        .short('m')
                        .long("metadata")
                        .value_name("???")
                        .takes_value(true),
                    Arg::new("uuid")
                        .about("Show dataset matching uuid")
                        .short('u')
                        .long("uuid")
                        .value_name("UUID")
                        .takes_value(true),
                    Arg::new("order")
                        .about("Sort results by field")
                        .short('o')
                        .long("order-by")
                        .value_name("FIELD.DIRECTION")
                        .possible_values(DatasetOrdering::VARIANTS)
                        .takes_value(true),
                    Arg::new("limit")
                        .about("Show N results (max 100)")
                        .short('l')
                        .long("limit")
                        .value_name("N")
                        .takes_value(true)
                        .validator(|val| {
                            match val.parse::<usize>().map_err(|e| {
                                clap::Error::with_description(
                                    format!("{}", e),
                                    clap::ErrorKind::InvalidValue,
                                )
                            })? {
                                1..=100 => Ok(()),
                                _ => Err(clap::Error::with_description(
                                    format!("Limit value must be between 1-100, got ({})", val),
                                    clap::ErrorKind::InvalidValue,
                                )),
                            }
                        }),
                    Arg::new("offset")
                        .about(
                            "Skip N results (WARNING: Results may shift between subsequent calls)",
                        )
                        .short('s')
                        .long("offset")
                        .value_name("N")
                        .takes_value(true),
                ]),
        )
        .subcommand(
            App::new("upload")
                .about("Upload file to remote dataset")
                .arg(Arg::new("dataset_uuid").required(true).takes_value(true))
                .arg(
                    Arg::new("file")
                        .about("File to upload to remote dataset")
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
