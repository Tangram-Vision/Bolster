//! Bolster is a Command Line Interface (CLI) from Tangram Vision for managing
//! sensor datasets and results of processing them.
//!
//! Bolster allows you to upload, download, and query datasets. Uploading
//! datasets will trigger processing in the cloud using our computer vision
//! algorithms. Processed results will be delivered via email and will also be
//! available for download via bolster.
//!
//! # Installation
//!
//! If you have [Rust installed](https://rustup.rs/), you can install bolster
//! with:
//!
//! ```shell
//! cargo install --branch=main --git=https://gitlab.com/tangram-vision-oss/bolster.git
//! ```
//!
//! Alternatively, release binaries are published for supported platforms at
//! <https://gitlab.com/tangram-vision-oss/bolster/-/releases/>.
//!
//! # Usage
//!
//! View CLI help with `bolster help` or `bolster help <subcommand>`.
//!
//! Bolster is intended to be used as a binary -- if you want to use it as a
//! library, [talk to us about your use case](https://tangram-vision.canny.io)!
//!
//! ## Configuration
//!
//! Bolster requires a configuration file to successfully interact with web
//! services. A configuration file is provided to you when you join the Alpha
//! program. To use the configuration file with bolster, either:
//!
//! - Place the configuration file at `~/.config/tangram_vision/bolster.toml`
//! - Use the `--config path/to/bolster.toml` flag
//!
//! ## Commands
//!
//! ```bolster config```
//!
//! Echoes current config (with any overrides applied) and exits.
//!
//! <br>
//!
//! ---
//!
//! ```bolster upload <system_id> <path>...```
//!
//! Creates a new dataset associated with the system ID and uploads all
//! files in the provided path(s). If any path is a directory, all files in the
//! directory will be uploaded. Folder structure is preserved when uploading to
//! cloud storage. Does not follow symlinks.
//!
//! Uploading files creates a new dataset and outputs the created dataset's
//! UUID, which can be used to download or query the dataset or the files it
//! contains in the future.
//!
//! The `<system_id>` provided when uploading a dataset should match however
//! you identify your systems/robots/installations, whether that be by an
//! integer (e.g. "unit 1") or a serial (e.g. "A12") or a build date (e.g.
//! "12-MAY-2021") or a location (e.g. "field3" or "southwest-corner") or
//! anything else. The dataset will be associated with the given system_id, to
//! allow filtering datasets (and processing results) by system.
//!
//! Note: Only files up to 4.88 TB may be uploaded.
//!
//! When uploading a dataset, filenames must be valid UTF-8 (this is a
//! requirement of cloud storage providers such as [AWS
//! S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)).
//!
//! ![Bolster upload example
//! gif](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-upload.gif)
//!
//! <br>
//!
//! ---
//!
//! ```bolster download <dataset_uuid> [prefix]...```
//!
//! Downloads files from the given dataset. Files to download may be filtered
//! by providing prefix(es). If multiple prefixes are provided, all files
//! matching any prefix will be downloaded.
//!
//! If downloading a file would overwrite an existing file, the user is
//! prompted to continue.
//!
//! ![Bolster download example
//! gif](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-download.gif)
//!
//! <br>
//!
//! ---
//!
//! ```bolster ls [OPTIONS]```
//!
//! List all datasets associated with your account. Datasets may be filtered
//! or sorted using various options (e.g. by creation date). If a specific
//! dataset is selected with the `--uuid` option, files in that dataset will be
//! listed.
//!
//! ![Bolster ls example
//! image](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-ls.png)
//!
//! ## Examples
//!
//! ```shell
//! ###############
//! # bolster upload
//! ###############
//!
//! # Uploads myfile1 as a new dataset for the "robot-1" system.
//! bolster upload robot-1 myfile1
//!
//! # Uploads myfile1, myfile2, and myfile3 as a new dataset for the "drone-A12"
//! # system.
//! bolster upload drone-A12 myfile1 myfile2 myfile3
//!
//! # Uploads all files in myfolder1 and myfile4 as a new dataset for "johnny-5"
//! # system.
//! bolster upload johnny-5 myfolder1 myfolder2/myfile4
//!
//! ###############
//! # bolster download
//! ###############
//!
//! # Downloads all files in dataset 1415fe36-851f-4c62-a616-4f5e343ba5fc to
//! # your current working directory.
//! bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc
//!
//! # Downloads files myfile1 and myfile2 from the dataset to your current
//! # working directory.
//! bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc myfile1 myfile2
//!
//! # Downloads all files in myfolder1 of the remote dataset into myfolder1 in
//! # your current working directory. myfolder1 is created if it does not
//! # exist.
//! bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc myfolder1
//!
//! ###############
//! # bolster ls
//! ###############
//!
//! # List 100 datasets instead of showing the default limit 20
//! bolster ls --limit=100
//!
//! # List all files in the specified dataset
//! bolster ls --uuid=1415fe36-851f-4c62-a616-4f5e343ba5fc
//!
//! # List datasets created in 2021 and sort them most-recent-first
//! bolster ls --after-date 2021-01-01 --order-by=created_date.desc
//! ```
//!
//! # Troubleshooting
//!
//! If you're encountering issues using bolster, please refer to the table below
//! for potential solutions. If the issue persists, please [let us
//! know](https://tangram-vision.canny.io).
//!
//! | Error                                     | Resolution                                                                                                                                                                                                                                                      |
//! |-                                          |-                                                                                                                                                                                                                                                                |
//! | Configuration file not found              | Bolster will use a configuration file located at `~/.config/tangram_vision/bolster.toml` by default. Alternately, provide a config file via the `--config` option, e.g. `bolster --config=path/to/bolster.toml ls`.                                             |
//! | Connection refused                        | Bolster upload/download/ls subcommands require an internet connection -- make sure your connection is working and that you can reach bolster.tangramvision.com and s3.us-west-1.amazonaws.com without interference or disruption from any firewalls or proxies. |
//! | All file/folder names must be valid UTF-8 | All filepaths uploaded as a dataset must be valid UTF-8 as required by S3-compatible cloud storage providers.                                                                                                                                                   |
//! | File/folder paths must be relative        | You may not use absolute filepaths with the upload sub-command, such as `/dir/file` or `~/dir/file`, because bolster preserves the folder structure of uploaded files.                                                                                          |
//!
//! # Security
//!
//! Bolster connects to web services using TLS (data is encrypted in transit).
//! Datasets stored with cloud storage providers are encrypted at rest. All
//! public access is blocked to the cloud storage bucket. Only the credentials
//! in the bolster config file provided to you (and a restricted set of Tangram
//! Vision processes and employees) have access to your datasets when uploaded
//! to cloud storage.
//!
//! If you would like to report a security bug or issue, email *support ~@~
//! tangramvision.com*. Our team will acknowledge your email within 72 hours,
//! and will send a more detailed response within 72 hours indicating the next
//! steps in handling your report. After the initial reply to your report, we
//! will endeavor to keep you informed of the progress towards a fix and full
//! announcement, and may ask for additional information or guidance.
//!
//! # Performance
//!
//! Bolster currently uploads up to 4 files in parallel with each file uploading
//! up to 10 separate 16-MB chunks at a time. So, bolster may use up to 640 MB
//! of RAM (plus some overhead). If you're working with a more constrained
//! environment, please [let us know](https://tangram-vision.canny.io).
//!
//! All uploaded files are md5-checksummed for data integrity. As a result, you
//! may notice some CPU load while uploading.
//!
//! # Feedback
//!
//! As always, if you have any feedback, please [let us
//! know](https://tangram-vision.canny.io/)!

#[cfg(not(debug_assertions))]
use human_panic::setup_panic;

mod app_config;
mod cli;
mod core;

use anyhow::Result;

#[doc(hidden)]
/// Main entrypoint
pub fn run() -> Result<()> {
    // Human Panic. Only enabled when *not* debugging.
    //
    // Example of what panic message looks like:
    // https://docs.rs/human-panic/1.0.3/human_panic/
    #[cfg(not(debug_assertions))]
    {
        setup_panic!();
    }

    // Better Panic. Only enabled *when* debugging.
    #[cfg(debug_assertions)]
    {
        better_panic::Settings::debug()
            .most_recent_first(false)
            .lineno_suffix(true)
            .verbosity(better_panic::Verbosity::Full)
            .install();
    }

    // Setup Logging
    // Used to use slog but switched to env_logger for simplicity.
    // https://gitlab.com/tangram-vision/bolster/-/merge_requests/4
    env_logger::init();

    // Get CLI arguments and flags (one may have provided the config file to use)
    let cli_matches = cli::cli_config()?;

    let mut settings = config::Config::default();
    // Use cmdline arg config file if provided, otherwise require config file at default ~/.config/... path
    if let Some(config_file) = cli_matches.value_of("config") {
        settings.merge(config::File::with_name(config_file))?;
    } else {
        settings.merge(config::File::with_name(&shellexpand::tilde(
            "~/.config/tangram_vision/bolster.toml",
        )))?;
    }

    // Override with environment variables, if present
    // Example of overriding: BOLSTER__AWS_S3__ACCESS_KEY=abc
    // (Note double underscore to reach into lower struct levels!)
    settings.merge(config::Environment::with_prefix("BOLSTER_").separator("__"))?;

    // Match against CLI subcommands, which delegate to functions
    cli::cli_match(settings, cli_matches)
}
