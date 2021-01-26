// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#[macro_use]
extern crate log;

pub mod commands;
pub mod error;
pub mod hazard;

use utils::error::Result;

pub fn start() -> Result<()> {
    // does nothing

    Ok(())
}
