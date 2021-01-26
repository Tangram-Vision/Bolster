// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#[cfg(test)]
extern crate assert_cmd;
extern crate predicates;

use assert_cmd::prelude::*;
use predicates::prelude::*;

use std::process::Command;

#[test]
fn test_cli() {
    let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
    cmd.assert().failure();
}

#[test]
fn test_hazard_exit_code() {
    let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
    cmd.arg("hazard").assert().code(0);
}

#[test]
fn test_hazard_stdout() {
    let hazard_predicate =
        predicate::function(|x: &str| x == "You got it right!\n" || x == "You got it wrong!\n");
    let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
    cmd.arg("hazard").assert().stdout(hazard_predicate);
}
