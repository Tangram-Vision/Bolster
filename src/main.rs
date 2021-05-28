// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

//! Bolster is a CLI from Tangram Vision for managing sensor datasets and
//! results of processing them.
//!
//! Bolster allows you to upload, download, and query datasets. Uploading
//! datasets will, in the near future, trigger processing in the cloud using our
//! computer vision algorithms. Processed results will be delivered via email
//! and will also be available for download via bolster.
//!
//! # Installation
//!
//! If you have [rust installed](https://rustup.rs/), you can install bolster
//! with:
//!
//! ```shell
//! # TODO: question: do we want to namespace all our public crates, e.g.
//! # tv-bolster or tangram-vision-bolster?
//! cargo install bolster
//! ```
//!
//! Alternatively, release binaries are published for supported platforms:
//! - [x86_64-unknown-linux-gnu](TODO-link-to-gitlab-releases)
//! - TODO: x86_64-pc-windows-msvc
//!
//! # Usage
//!
//! View CLI help with `bolster help` or `bolster help <subcommand>`.
//!
//! Bolster is intended to be used as a binary -- if you want to use it as a
//! library, [talk to us about your
//! usecase](https://tangram-vision.canny.io)!
//!
//! ## Configuration
//!
//! Bolster requires a configuration file to successfully interact with web
//! services. A configuration file is provided to you when you join the Alpha
//! program. To use the configuration file with bolster, either:
//! - place the configuration file at `~/.config/tangram_vision/bolster.toml` or
//! - use the `--config path/to/bolster.toml` flag
//!
//! ## Subcommands
//!
//! Bolster provides several subcommands:
//!
//! | Subcommand | Description |
//! |-|-|
//! | `bolster config` | Echoes current config (with any overrides applied) and exits. |
//! | `bolster create <PATH>...` | Creates a new dataset and uploads all files in the provided PATH(s). If any PATH is a directory, all files in the directory will be uploaded. Folder structure is preserved when uploading to cloud storage. Does not follow symlinks. |
//! | `bolster download <dataset_uuid> [prefix]...` | Downloads files from the given dataset. Files to download may be filtered by providing prefix(es). If multiple prefixes are provided, all files matching any prefix will be downloaded. |
//! | `bolster ls [OPTIONS]` | List all datasets associated with your account. Datasets may be filtered or sorted using various options (e.g. by creation date). If a specific dataset is selected with the `--uuid` option, files in that dataset will be listed. |
//!
//! When uploading a dataset, **filenames must be valid UTF-8** (this is a
//! requirement of cloud storage providers such as [AWS
//! S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)).
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
//! If you would like to report a security bug or issue, email
//! security@tangramvision.com (TODO: should we make this email list?). Our team
//! will acknowledge your email within 72 hours, and will send a more detailed
//! response within 72 hours indicating the next steps in handling your report.
//! After the initial reply to your report, we will endeavor to keep you
//! informed of the progress towards a fix and full announcement, and may ask
//! for additional information or guidance.
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

/// Runs the binary!
fn main() -> Result<()> {
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
