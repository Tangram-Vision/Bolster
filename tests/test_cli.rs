// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#[cfg(test)]
extern crate assert_cmd;

use assert_cmd::prelude::*;

use std::process::Command;

#[test]
fn test_cli() {
    let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
    cmd.assert().failure();
}
