// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use anyhow::Result;
use std::fs::File;

/// Return, randomly, true or false
pub fn simulate_error() -> Result<()> {
    // Trigger an error
    File::open("thisfiledoesnotexist")?;

    Ok(())
}
