// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::{anyhow, Result};
use byte_unit::Byte;
use chrono::NaiveDate;
use clap::{crate_authors, crate_description, crate_version};
use clap::{App, AppSettings, Arg};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use strum::VariantNames;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::app_config::{DatabaseConfig, StorageProviderChoices};
use crate::core::api::datasets::{DatabaseApiConfig, DatasetGetRequest, DatasetOrdering};
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
#[tokio::main]
pub async fn cli_match(config: config::Config, cli_matches: clap::ArgMatches) -> Result<()> {
    // Handle config subcommand first, because it doesn't need any valid configuration, and is helpful for debugging bad config!
    if let Some(("config", _config_matches)) = cli_matches.subcommand() {
        commands::print_config(config)?;
        return Ok(());
    }

    // Derive config needed for all commands (they all interact with the database)
    let db = config.clone().try_into::<DatabaseConfig>()?.database;
    let db_config = DatabaseApiConfig::new(db.url.clone(), db.jwt.clone())?;

    // Handle all subcommands that interact with database or storage
    match cli_matches.subcommand() {
        Some(("create", create_matches)) => {
            let provider =
                StorageProviderChoices::from_str(create_matches.value_of("provider").unwrap())?;
            let file_pathbufs: Vec<PathBuf> = create_matches
                .values_of_os("PATH")
                .unwrap()
                .map(|os_str| Path::new(os_str))
                .collect::<Vec<&Path>>()
                .iter_mut()
                .try_fold(Vec::new(), |mut acc, path| -> Result<Vec<PathBuf>> {
                    let file_list: Result<Vec<PathBuf>> = match path {
                        // WalkDir does not follow symlinks by default
                        path if path.is_dir() => Ok(WalkDir::new(path)
                            .into_iter()
                            .filter_map(Result::ok)
                            .filter(|entry| entry.file_type().is_file())
                            .map(|entry| entry.into_path())
                            .collect::<Vec<PathBuf>>()),
                        path if path.is_file() => Ok(vec![path.to_path_buf()]),
                        _ => Err(anyhow!("File path {:?} is not a directory or a file", path)),
                    };
                    let mut file_list = file_list?;
                    acc.append(&mut file_list);
                    Ok(acc)
                })?;
            let file_paths: Vec<&Path> = file_pathbufs
                .iter()
                .map(|pathbuf| pathbuf.as_path())
                .collect();

            let skip_prompt = create_matches.is_present("yes");
            if skip_prompt {
                println!("Creating a dataset of {} file(s)", file_paths.len());
            } else {
                println!(
                    "This command will create a dataset of {} file(s):",
                    file_paths.len()
                );
                println!(
                    "\t{}",
                    file_paths
                        .iter()
                        .map(|path| format!("{:?}", path))
                        .collect::<Vec<String>>()
                        .join("\n\t")
                );
                print!("Continue? (y/n) ");
                io::stdout().flush()?;

                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                if !input.to_lowercase().starts_with('y') {
                    return Ok(());
                }
            }
            // for each path,
            //   if it's a folder, collect all files inside recursively
            //   if it's a file, collect it
            // prompt that file list is correct
            // pass file list to command

            // TODO: test non-utf8 filename or force utf8
            let storage_config = storage::StorageConfig::new(config, provider)?;
            let prefix = db.user_id_from_jwt()?.to_string();
            commands::create_and_upload_dataset(storage_config, &db_config, &prefix, file_paths)
                .await?;
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

            // TODO: implement metadata CLI input

            let dataset_id: Option<Uuid> = handle_optional_arg(ls_matches, "dataset_uuid");
            let limit: Option<usize> = handle_optional_arg(ls_matches, "limit");
            let offset: Option<usize> = handle_optional_arg(ls_matches, "offset");

            // TODO: implement order
            let order: Option<DatasetOrdering> = handle_optional_arg(ls_matches, "order");

            let get_params = DatasetGetRequest {
                dataset_id,
                before_date,
                after_date,
                order,
                limit,
                offset,
            };

            let datasets = commands::list_datasets(&db_config, &get_params).await?;

            if datasets.is_empty() {
                println!("No datasets found!");
            } else {
                // TODO: use generic, customizable formatter (e.g. kubernetes get)
                // TODO: show creator for tangram-internal build

                // If user is listing a single dataset, show its files...
                if let Some(dataset_id) = dataset_id {
                    if datasets[0].files.is_empty() {
                        println!("No files found in dataset {}", dataset_id.to_string());
                    } else {
                        println!("Files in dataset {}:\n", dataset_id.to_string());
                        println!("{:<32} {:<12} URL", "Created Datetime", "Filesize",);
                        for f in &datasets[0].files {
                            println!(
                                "{:<32} {:<12} {}",
                                f.created_date.to_string(),
                                Byte::from_bytes(f.filesize as u128)
                                    .get_appropriate_unit(false)
                                    .to_string(),
                                f.url,
                            );
                        }
                    }
                }
                // ... otherwise show just datasets
                else {
                    println!(
                        "{:<40} {:<32} {:<8} {:<12}",
                        "UUID", "Created Datetime", "# Files", "Filesize",
                    );
                    for d in datasets {
                        println!(
                            "{:<40} {:<32} {:<8} {:<12}",
                            d.dataset_id.to_string(),
                            d.created_date.to_string(),
                            d.files.len(),
                            Byte::from_bytes(
                                d.files.iter().fold(0, |acc, x| acc + x.filesize as u128)
                            )
                            .get_appropriate_unit(false)
                            .to_string()
                        );
                    }
                }
            }
        }
        Some(("upload", upload_matches)) => {
            // Safe to unwrap because arguments are required or have defaults
            let dataset_id: Uuid = upload_matches.value_of_t_or_exit("dataset_uuid");
            let input_file = upload_matches.value_of("file").unwrap();
            let provider =
                StorageProviderChoices::from_str(upload_matches.value_of("provider").unwrap())?;
            let storage_config = storage::StorageConfig::new(config, provider)?;
            let prefix = db.user_id_from_jwt()?.to_string();
            commands::upload_file(
                storage_config,
                &db_config,
                dataset_id,
                Path::new(input_file),
                &prefix,
            )
            .await?;
        }
        Some(("download", download_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_id: Uuid = download_matches.value_of_t_or_exit("dataset_uuid");
            let filename = download_matches.value_of("filename").unwrap();
            let files = commands::list_files(&db_config, dataset_id, filename).await?;
            if files.is_empty() {
                return Err(anyhow!(
                    "No files in dataset {} matched the filename {}",
                    dataset_id,
                    filename
                ));
            } else {
                let file = &files[0];
                // TODO: support downloading many files
                commands::download_file(config, &file.url).await?;
            }
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
        .subcommand(
            App::new("create")
                .about("Create + upload a new dataset")
                .arg(
                    Arg::new("PATH")
                        .about("Path(s) to folder(s) or file(s) to upload")
                        .required(true)
                        .takes_value(true)
                        .multiple(true)
                )
                .arg(
                    Arg::new("yes")
                        .about("Automatic yes to prompt that lists files to upload")
                        .short('y')
                        .long("yes")
                        .takes_value(true)
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
                // TODO: add -y/--yes to skip prompt
        )
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
                    // TODO: implement metadata
                    Arg::new("metadata")
                        .about("NOT IMPLEMENTED: Show dataset matching metadata")
                        .short('m')
                        .long("metadata")
                        .value_name("???")
                        .takes_value(true),
                    Arg::new("dataset_uuid")
                        .about("Show files in dataset matching uuid")
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
                        .default_value("20")
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
                .arg(Arg::new("dataset_uuid").required(true).takes_value(true))
                .arg(Arg::new("filename").about("Filename of file to download").required(true).takes_value(true))
            // TODO: add arg to filter file(s) to download from dataset?
            // TODO: add path to download files to?
        )
        .subcommand(App::new("config").about("Show Configuration"));

    // Get matches
    let cli_matches = cli_app.get_matches();

    Ok(cli_matches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_database_jwt() {
        // Initialize configuration
        let mut config = config::Config::default();
        config
            .merge(config::File::from_str(
                "[database]\n",
                config::FileFormat::Toml,
            ))
            .unwrap();
        let error = cli_match(config, clap::ArgMatches::default())
            .expect_err("Expected error due to missing database jwt");
        assert_eq!(error.to_string(), "missing field `jwt`");
    }

    // Other CLI-related tests are in tests/test_cli.rs and act as integration
    // tests (running the whole bolster binary) so they can properly test the
    // ClapError.exit functionality when CLI args are malformed.
}
