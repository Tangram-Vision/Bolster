// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

use assert_cmd::prelude::*;
use httpmock::Method::GET;
use httpmock::MockServer;
use predicates::prelude::*;
use serde_json::json;
use std::process::Command;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        cmd.assert().failure();
    }

    #[test]
    fn test_cli_env_var_overrides_file_config() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("config")
            .env("BOLSTER__DATABASE__JWT", "a different jwt")
            .assert()
            .success()
            .stdout(predicate::str::contains("a different jwt"));
    }

    #[test]
    fn test_cli_validates_uuid_format() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("ls")
            .arg("--uuid=not-a-real-uuid")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "isn't a valid value for 'uuid': invalid length",
            ));
    }

    #[test]
    fn test_cli_validates_date_format() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("ls")
            .arg("--after-date=whatever")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "input contains invalid characters",
            ));
    }

    #[test]
    fn test_cli_validates_date_bounds() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("ls")
            .arg("--after-date=2021-01-01")
            .arg("--before-date=2020-01-01")
            .assert()
            .failure()
            .stderr(
                predicate::str::is_match("before_date.*must be later than the after_date").unwrap(),
            );
    }

    #[test]
    fn test_cli_filtering_by_creator_unavailable() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("ls")
            .arg("--creator=tangram_user")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Found argument '--creator' which wasn't expected, or isn't valid in this context",
            ));
    }

    #[test]
    fn test_cli_digitalocean_provider_unavailable() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("upload")
            .arg("--provider=digitalocean")
            .arg("25a017c2-f371-4fd6-8973-62034bf6bed9")
            .arg("non-existent-file")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "'digitalocean' isn't a valid value for '--provider <PROVIDER>'",
            ));
    }
}

#[cfg(all(test, feature = "tangram-internal"))]
mod tests_internal {
    use super::*;

    #[test]
    fn test_cli_filtering_by_creator_available() {
        // To debug what rusoto and httpmock are doing, enable logger and run
        // tests with debug or trace level.
        // let _ = env_logger::try_init();

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer abc")
                .query_param("creator_role", "eq.tangram_user")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([]));
        });

        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("ls")
            .arg("--creator=tangram_user")
            .env("BOLSTER__DATABASE__URL", server.base_url())
            .assert()
            .success()
            .stdout(predicate::str::contains("No datasets found!"));
        mock.assert();
    }

    #[test]
    fn test_cli_digitalocean_provider_available() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("src/resources/test_full_config.toml")
            .arg("upload")
            .arg("--provider=digitalocean")
            .arg("25a017c2-f371-4fd6-8973-62034bf6bed9")
            .arg("non-existent-file")
            .assert()
            .failure()
            .stderr(predicate::str::contains("Error: No such file or directory"));
    }
}
