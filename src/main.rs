// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#[cfg(not(debug_assertions))]
use human_panic::setup_panic;

mod app_config;
mod cli;
mod core;

use anyhow::Result;

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
        settings.merge(config::File::with_name(
            "~/.config/tangramvision/bolster.toml",
        ))?;
    }

    // Override with environment variables, if present
    // Example of overriding: BOLSTER__AWS_S3__ACCESS_KEY=abc
    // (Note double underscore to reach into lower struct levels!)
    settings.merge(config::Environment::with_prefix("BOLSTER_").separator("__"))?;

    // Match against CLI subcommands, which delegate to functions
    cli::cli_match(settings, cli_matches)
}
