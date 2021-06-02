//! Bolster is a CLI from Tangram Vision for managing sensor datasets and
//! results of processing them.
//!
//! See [bolster] for further documentation.

use anyhow::Result;

/// Runs the binary!
fn main() -> Result<()> {
    bolster::run()
}
