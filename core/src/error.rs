use std::fs::File;
use utils::error::Result;

/// Return, randomly, true or false
pub fn simulate_error() -> Result<()> {
    // Trigger an error
    File::open("thisfiledoesnotexist")?;

    Ok(())
}
