//! Command-line interface (subcommands, arguments, and handling)
//!
//! For overall architecture, see [ARCHITECTURE.md](https://gitlab.com/tangram-vision/oss/bolster/-/blob/main/ARCHITECTURE.md)

use std::{
    ffi::OsStr,
    fmt::Display,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    str::FromStr,
};

use anyhow::{anyhow, bail, Result};
use byte_unit::Byte;
use chrono::NaiveDate;
use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg};
use strum::VariantNames;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{
    app_config::{DatabaseConfig, StorageProviderChoices},
    core::{
        api::{
            datasets::{DatabaseApiConfig, DatasetGetRequest, DatasetOrdering},
            storage,
            storage::StorageConfig,
        },
        commands,
    },
};

/// If trying to upload more files, exit and prompt to tar/zip files.
const UPLOAD_MAX_FILES_ALLOWED: usize = 200;

/// Extract optional arg with a specific type, exiting on parse error.
pub fn handle_optional_arg<T>(matches: &clap::ArgMatches, arg_name: &str) -> Option<T>
where
    T: FromStr,
    <T as FromStr>::Err: Display,
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

/// Different kinds of paths that bolster expects as arguments
#[derive(Debug)]
pub enum PathKind {
    /// Plex (associated path should point to a .plex file)
    Plex,
    /// Object-space CSV (associated path should point to a .csv file)
    ObjectSpaceCsv,
    /// Data (associated path(s) should point to a .bag file or folders)
    Data,
}

impl PathKind {
    /// Validates that the given path matches expectations for the PathKind
    ///
    /// # Errors
    ///
    /// - For [PathKind::Plex], an error is raised if the path doesn't end in
    /// `.plex` or if the path points to a non-existent or unreadable file.
    /// - For [PathKind::ObjectSpaceCsv], an error is raised if the path doesn't
    /// end in `.csv` or if the path points to a non-existent or unreadable file.
    /// - For [PathKind::Data], an error is raised if the path points to a file
    /// but the file doesn't end in `.bag`, or if the path points to an
    /// unreadable file or directory, or if the path points to a non-existent
    /// file/folder.
    pub fn validate(self, path: &Path) -> Result<()> {
        match self {
            PathKind::Plex => {
                if path
                    .extension()
                    .unwrap_or_else(|| OsStr::new(""))
                    .to_ascii_lowercase()
                    != "plex"
                {
                    bail!("Plex file ({:?}) doesn't end in .plex", path);
                }
                if !path.is_file() {
                    bail!("Plex file ({:?}) does not exist or is unreadable", path);
                }
                Ok(())
            }
            PathKind::ObjectSpaceCsv => {
                if path
                    .extension()
                    .unwrap_or_else(|| OsStr::new(""))
                    .to_ascii_lowercase()
                    != "csv"
                {
                    bail!("Object-space CSV file ({:?}) doesn't end in .csv", path);
                }
                if !path.is_file() {
                    bail!(
                        "Object-space CSV file ({:?}) does not exist or is unreadable",
                        path
                    );
                }
                Ok(())
            }
            PathKind::Data => {
                if path.is_file() {
                    if path
                        .extension()
                        .unwrap_or_else(|| OsStr::new(""))
                        .to_ascii_lowercase()
                        != "bag"
                    {
                        bail!(
                            "Data file ({:?}) doesn't end in .bag. Data input \
                            must be .bag files or folders.",
                            path
                        );
                    }
                } else if path.is_dir() {
                    // Maybe eventually we'll parse the plex and ensure the
                    // folder names match.
                } else {
                    bail!("Data file ({:?}) does not exist or is unreadable", path);
                }
                Ok(())
            }
        }
    }
}

/// Ensures paths are relative, free of ./.., utf-8, existing, and with the correct extension.
///
/// # Errors
///
/// Returns an error if any provided paths (i.e. for the plex, csv, or data paths):
/// - Are absolute
/// - Contain `.` (current directory) or `..` (parent directory)
/// - Are not valid UTF-8
/// - Do not exist (plex and csv arguments must point to a file, data arguments
/// must point to a file or folder)
/// - Have the wrong extension (the plex argument must be a file ending with
/// .plex, the object space csv argument must be a file ending with .csv)
pub fn clean_and_validate_path(path_os_str: &OsStr, path_kind: PathKind) -> Result<String> {
    let path = Path::new(path_os_str);
    path_kind.validate(path)?;
    // Ensure plex path does not contain . or ..
    if path
        .components()
        .any(|p| p == Component::CurDir || p == Component::ParentDir)
    {
        bail!(
            "Paths must not contain './' or '../'. (Folder structure is \
            preserved in the cloud, so uploading `dir/file` will create \
            a file at a different location than doing `cd dir` then \
            uploading `file`.)"
        );
    }
    if path.is_absolute() {
        bail!(
            "File/folder paths must be relative! (Folder structure is \
            preserved in the cloud, so uploading `dir/file` will create \
            a file at a different location than doing `cd dir` then \
            uploading `file`.)"
        );
    }
    // Require all paths to be UTF-8 encodable, because S3 requires UTF-8
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    let utf8_path = path.to_str().ok_or_else(||
        anyhow!("All file/folder names must be valid UTF-8 (AWS S3 requirement). Invalid UTF-8: {:?}", path)
    )?.to_owned();

    Ok(utf8_path)
}

/// Process provided CLI subcommands and options.
///
/// # Errors
///
/// Exits with an error message if any command-line arguments are missing but
/// required or if arguments are malformed (e.g. expected a UUID but the
/// provided value isn't one).
///
/// Returns an error if creating a dataset and the provided filepaths are
/// absolute (they must be relative so folder structure can be preserved in
/// cloud storage) or if any filepaths are not valid UTF-8.
///
/// Returns an error if any lower-level commands (e.g. for uploading or
/// downloading)
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
        Some(("upload", upload_matches)) => {
            let provider =
                StorageProviderChoices::from_str(upload_matches.value_of("provider").unwrap())?;
            let storage_config = storage::StorageConfig::new(config, provider)?;
            let prefix = db.user_id_from_jwt()?.to_string();

            let system_id: String = upload_matches.value_of_t_or_exit::<String>("system_id");
            let plex_path = upload_matches.value_of_os("plex_path").unwrap();
            let utf8_plex_path = clean_and_validate_path(plex_path, PathKind::Plex)?;

            let csv_path = upload_matches.value_of_os("object_space_csv_path").unwrap();
            let utf8_csv_path = clean_and_validate_path(csv_path, PathKind::ObjectSpaceCsv)?;

            let file_paths: Vec<&OsStr> = upload_matches.values_of_os("path").unwrap().collect();
            let mut utf8_file_paths: Vec<String> = file_paths
                .iter()
                .map(|os_str| clean_and_validate_path(os_str, PathKind::Data))
                .collect::<Result<Vec<String>>>()?;

            // Collect utf8 paths to all files in any provided data folders (including subfolders)
            let mut all_utf8_file_paths: Vec<String> = utf8_file_paths
                .iter_mut()
                .try_fold(Vec::new(), |mut acc, utf8_path| -> Result<Vec<PathBuf>> {
                    let path = Path::new(utf8_path);
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
                })?
                .iter()
                .map(|pathbuf| Ok(pathbuf.as_path().to_str().ok_or_else(||
                    anyhow!("All file/folder names must be valid UTF-8 (AWS S3 requirement). Invalid UTF-8: {:?}", pathbuf)
                )?.to_owned()))
                .collect::<Result<Vec<String>>>()?;

            if all_utf8_file_paths.len() > UPLOAD_MAX_FILES_ALLOWED {
                bail!("You're trying to upload {} files (max = {}). Please tar/zip the files before uploading!", all_utf8_file_paths.len(), UPLOAD_MAX_FILES_ALLOWED);
            }

            // Add the CSV path in with all the data paths. We don't track the
            // CSV separately (as we do the plex) because we don't anticipate
            // querying by CSV or tracking different categories of CSVs as we do
            // with plexes (e.g.  calibrated vs uncalibrated).
            all_utf8_file_paths.insert(0, utf8_csv_path);

            let skip_prompt = upload_matches.is_present("yes");
            if skip_prompt {
                println!(
                    "Creating a dataset of {} file(s)",
                    all_utf8_file_paths.len()
                );
            } else {
                println!(
                    "This command will create a dataset with a plex, a csv, and {} data file(s):",
                    all_utf8_file_paths.len()
                );
                println!(
                    "\t{}\n\t{}",
                    utf8_plex_path,
                    all_utf8_file_paths.join("\n\t")
                );
                print!("Continue? [y/n] ");
                io::stdout().flush()?;

                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                if !input.to_lowercase().starts_with('y') {
                    return Ok(());
                }
            }

            commands::create_and_upload_dataset(
                storage_config,
                &db_config,
                system_id,
                &prefix,
                utf8_plex_path,
                all_utf8_file_paths,
            )
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

            // TODO: Implement metadata CLI input
            // Related to
            // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/1
            // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/4

            let dataset_id: Option<Uuid> = handle_optional_arg(ls_matches, "dataset_uuid");
            let system_id: Option<String> = handle_optional_arg(ls_matches, "system_id");
            let limit: Option<usize> = handle_optional_arg(ls_matches, "limit");
            let offset: Option<usize> = handle_optional_arg(ls_matches, "offset");

            let order: Option<DatasetOrdering> = handle_optional_arg(ls_matches, "order");

            let get_params = DatasetGetRequest {
                dataset_id,
                system_id,
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
                // If user is listing a single dataset, show its files...
                if let Some(dataset_id) = dataset_id {
                    if datasets[0].files.is_empty() {
                        println!("No files found in dataset {}", dataset_id);
                    } else {
                        println!("Files in dataset {}:\n", dataset_id);
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
                        "{:<40} {:<20.18} {:<26} {:<8} {:<12}",
                        "UUID", "System ID", "Created Datetime", "# Files", "Filesize",
                    );
                    for d in datasets {
                        println!(
                            "{:<40} {:<20.18} {:<26} {:<8} {:<12}",
                            d.dataset_id.to_string(),
                            d.system_id,
                            d.created_date.format("%Y-%m-%d %H:%M:%S UTC"),
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
        Some(("download", download_matches)) => {
            // Safe to unwrap because argument is required
            let dataset_id: Uuid = download_matches.value_of_t_or_exit("dataset_uuid");
            let prefixes = download_matches
                .values_of("prefix")
                .map_or_else(Vec::new, |values| {
                    values.map(|s| s.to_owned()).collect::<Vec<String>>()
                });
            let uploaded_files = commands::list_files(&db_config, dataset_id, prefixes).await?;

            // Based on url from database, find which StorageProvider's config to use
            let provider = StorageProviderChoices::from_url(&uploaded_files[0].url)?;
            let storage_config = StorageConfig::new(config, provider)?;

            let total_filesize = uploaded_files.iter().fold(0, |acc, f| acc + f.filesize);
            let number_of_files = uploaded_files.len();

            println!(
                "Downloading {} files, total {}",
                number_of_files,
                Byte::from_bytes(total_filesize as u128).get_appropriate_unit(false)
            );

            for file in uploaded_files.iter() {
                let filepath = file.filepath_from_url()?;

                // TODO: add --force flag to skip prompt
                if filepath.exists() {
                    print!("Overwrite file: {} ? [y/n]", filepath.as_path().display());
                    io::stdout().flush()?;

                    let mut input = String::new();
                    io::stdin().read_line(&mut input)?;
                    if !input.to_lowercase().starts_with('y') {
                        return Ok(());
                    }
                }
            }
            commands::download_files(storage_config, uploaded_files).await?;
        }
        _ => {
            // Arguments are required by default (in Clap).
            // This section should never execute.
            unreachable!("No matching subcommand!");
        }
    }
    Ok(())
}

/// Configures CLI arguments and help messages.
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
            App::new("upload")
                .about("Upload files, creating a new remote dataset")
                .arg(
                    Arg::new("system_id")
                        .about("String that identifies the \
                                system/device/robot/installation that produced \
                                the dataset. Useful for filtering datasets and \
                                results.")
                        .value_name("SYSTEM_ID")
                        .required(true)
                        .takes_value(true)
                )
                .arg(
                    Arg::new("plex_path")
                        .about("Path to .plex file describing system's sensor \
                                configuration.")
                        .value_name("PLEX_PATH")
                        .required(true)
                        .takes_value(true)
                )
                .arg(
                    Arg::new("object_space_csv_path")
                        .about("Path to .csv file describing object space.")
                        .value_name("OBJECT_SPACE_CSV_PATH")
                        .required(true)
                        .takes_value(true)
                )
                .arg(
                    Arg::new("path")
                        .about("Path to .bag file (where topic names of data \
                                streams must match component names in the plex) \
                                or path(s) to folder(s) containing data (folder \
                                names must match component names in the plex).")
                        .value_name("PATH")
                        .required(true)
                        .takes_value(true)
                        .multiple(true)
                )
                .arg(
                    Arg::new("yes")
                        .about("Automatic yes to prompt that lists files to upload")
                        .short('y')
                        .long("yes")
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
                    // TODO: Implement metadata CLI input
                    // Related to
                    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/1
                    // - https://gitlab.com/tangram-vision/oss/bolster/-/issues/4
                    // Arg::new("metadata")
                    //     .about("NOT IMPLEMENTED: Show dataset matching metadata")
                    //     .short('m')
                    //     .long("metadata")
                    //     .value_name("???")
                    //     .takes_value(true),
                    Arg::new("dataset_uuid")
                        .about("Show files in dataset matching uuid")
                        .short('u')
                        .long("uuid")
                        .value_name("UUID")
                        .takes_value(true),
                    Arg::new("system_id")
                        .about("Show datasets from specified system")
                        .short('d')
                        .long("system-id")
                        .value_name("SYSTEM_ID")
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
            App::new("download")
                .about("Download files in remote dataset")
                .args(&[
                    Arg::new("dataset_uuid")
                        .value_name("DATASET_UUID")
                        .required(true)
                        .takes_value(true),
                    Arg::new("prefix")
                        .value_name("PREFIX")
                        .about("All files with names starting with a prefix will be downloaded")
                        .takes_value(true)
                        .multiple(true)
                ])
            // TODO: Add path to download files to?
        )
        .subcommand(App::new("config").about("Show Configuration"));

    // Get matches
    let cli_matches = cli_app.get_matches();

    Ok(cli_matches)
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

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

    #[test]
    fn test_plex_pathkind_validation_good() {
        let path = Path::new("src/resources/test.plex");
        PathKind::Plex.validate(path).unwrap();
    }

    #[test]
    fn test_plex_pathkind_validation_bad_extension() {
        let path = Path::new("src/resources/test_full_config.toml");
        PathKind::Plex.validate(path).unwrap_err();
    }

    #[test]
    fn test_plex_pathkind_validation_nonexistent() {
        let path = Path::new("non-existent.plex");
        PathKind::Plex.validate(path).unwrap_err();
    }

    #[test]
    fn test_csv_pathkind_validation_good() {
        let path = Path::new("src/resources/test.csv");
        PathKind::ObjectSpaceCsv.validate(path).unwrap();
    }

    #[test]
    fn test_csv_pathkind_validation_bad_extension() {
        let path = Path::new("src/resources/test_full_config.toml");
        PathKind::ObjectSpaceCsv.validate(path).unwrap_err();
    }

    #[test]
    fn test_csv_pathkind_validation_nonexistent() {
        let path = Path::new("non-existent.csv");
        PathKind::ObjectSpaceCsv.validate(path).unwrap_err();
    }

    #[test]
    fn test_data_pathkind_validation_good_bag() {
        let path = Path::new("src/resources/test.bag");
        PathKind::Data.validate(path).unwrap();
    }

    #[test]
    fn test_data_pathkind_validation_good_folder() {
        let path = Path::new("src/resources");
        PathKind::Data.validate(path).unwrap();
    }

    #[test]
    fn test_data_pathkind_validation_bad_extension() {
        let path = Path::new("src/resources/test_full_config.toml");
        PathKind::Data.validate(path).unwrap_err();
    }

    #[test]
    fn test_data_pathkind_validation_nonexistent() {
        let path = Path::new("non-existent.bag");
        PathKind::Data.validate(path).unwrap_err();
    }

    #[test]
    fn test_clean_and_validate_success() {
        let path = OsStr::new("src/resources/test.plex");
        clean_and_validate_path(path, PathKind::Plex).unwrap();
    }

    #[test]
    fn test_clean_and_validate_disallow_dots() {
        let path = OsStr::new("src/../src/resources/test.plex");
        clean_and_validate_path(path, PathKind::Plex).unwrap_err();
    }

    #[test]
    fn test_clean_and_validate_disallow_absolute_path() {
        let path = Path::new("src/resources/test.plex")
            .canonicalize()
            .unwrap()
            .into_os_string();
        clean_and_validate_path(&path, PathKind::Plex).unwrap_err();
    }

    #[test]
    fn test_clean_and_validate_disallow_non_utf8() {
        let pathbuf = PathBuf::from(OsString::from_vec(vec![255]));
        std::fs::write(pathbuf.as_path(), "bolster test").unwrap();

        let path = pathbuf.as_os_str();
        clean_and_validate_path(path, PathKind::Plex).unwrap_err();
    }

    // Other CLI-related tests are in tests/test_cli.rs and act as integration
    // tests (running the whole bolster binary) so they can properly test the
    // ClapError.exit functionality when CLI args are malformed.
}
