// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#[cfg(not(debug_assertions))]
use human_panic::setup_panic;

#[cfg(debug_assertions)]
extern crate better_panic;

mod cli;
mod core;
mod utils;

use anyhow::Result;
use utils::app_config::AppConfig;

fn main() -> Result<()> {
    // Human Panic. Only enabled when *not* debugging.
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
    // Don't move this to logger.rs, because then the logger and guard will go
    // out of scope and be destructed/dropped!
    let _guard = slog_scope::set_global_logger(utils::logger::default_root_logger()?);
    let _log_guard = slog_stdlog::init()?;

    // Initialize Configuration with defaults
    let config_contents = include_str!("resources/default_config.toml");
    AppConfig::init(Some(config_contents))?;

    // Match Commands
    cli::cli_match()
}
