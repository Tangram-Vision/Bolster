// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use crate::utils::error::Result;

/// Return, randomly, true or false
pub fn generate_hazard() -> Result<bool> {
    Ok(rand::random())
}
