#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        os::unix::ffi::OsStringExt,
        path::{Path, PathBuf},
    };

    use assert_cmd::Command;
    use httpmock::{Method::GET, MockServer};
    use predicates::prelude::*;
    use serde_json::json;

    #[test]
    fn test_cli() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        cmd.assert().failure();
    }

    #[test]
    fn test_cli_env_var_overrides_file_config() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
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
            .arg("fixtures/test_full_config.toml")
            .arg("ls")
            .arg("--uuid=not-a-real-uuid")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "isn't a valid value for 'dataset_uuid': invalid length",
            ));
    }

    #[test]
    fn test_cli_validates_date_format() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
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
            .arg("fixtures/test_full_config.toml")
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
            .arg("fixtures/test_full_config.toml")
            .arg("ls")
            .arg("--creator=tangram_user")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Found argument '--creator' which wasn't expected, or isn't valid in this context",
            ));
    }

    #[test]
    fn test_cli_no_files_in_dataset() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiODA3Y2ZmZTUtZGY2ZC00MzRhLTg2YTQtZDAwN2NkNzQ2YmQzIn0.761nFCTaAsLnU-VaUrLDMNKL6VffxEL9acYbYIaT7tQ")
                .query_param("dataset_id", "eq.26fb2ac2-642a-4d7e-8233-b1835623b46b")
                .path("/datasets");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "dataset_id": "26fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    "system_id": "robot-1",
                    "metadata": {
                        "description": "Test"
                    },
                    "files": [],
                }]));
        });

        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("ls")
            .arg("--uuid=26fb2ac2-642a-4d7e-8233-b1835623b46b")
            .env("BOLSTER__DATABASE__URL", server.base_url())
            .assert()
            .success()
            .stdout(predicate::str::contains("No files found in dataset"));
        mock.assert();
    }

    #[test]
    fn test_cli_upload_disallows_absolute_filepath() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("fixtures/empty.bag").canonicalize().unwrap();
        assert!(filepath.is_absolute());

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "File/folder paths must be relative!",
            ));
    }
    #[test]
    fn test_cli_upload_disallows_non_utf8() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        // path is '255'.bag
        let pathbuf = PathBuf::from(OsString::from_vec(vec![255, 46, 98, 97, 103]));
        std::fs::write(pathbuf.as_path(), "bolster test").unwrap();

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(pathbuf)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "All file/folder names must be valid UTF-8",
            ));
    }
    #[test]
    fn test_cli_upload_disallows_non_utf8_plex_path() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let filepath = Path::new("fixtures/empty.bag");
        let toml_filepath = Path::new("fixtures/empty.toml");
        // path is '255'.plex
        let plex_pathbuf = PathBuf::from(OsString::from_vec(vec![255, 46, 112, 108, 101, 120]));
        std::fs::write(plex_pathbuf.as_path(), "bolster test").unwrap();

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_pathbuf)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "All file/folder names must be valid UTF-8",
            ));
    }

    #[test]
    fn test_cli_upload_lists_files_and_prompts() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/example.plex");
        let toml_filepath = Path::new("fixtures/checkerboard_detector.toml");
        let filepath = Path::new("fixtures/empty.bag");
        assert!(filepath.is_relative());

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .write_stdin("n")
            .assert()
            .success()
            .stdout(predicate::str::contains(filepath.to_str().unwrap()))
            .stdout(predicate::str::contains("Continue? [y/n]"));
    }

    #[test]
    fn test_cli_download_outputs_num_files_and_bytes_and_prompts() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiODA3Y2ZmZTUtZGY2ZC00MzRhLTg2YTQtZDAwN2NkNzQ2YmQzIn0.761nFCTaAsLnU-VaUrLDMNKL6VffxEL9acYbYIaT7tQ")
                .query_param("dataset_id", "eq.26fb2ac2-642a-4d7e-8233-b1835623b46b")
                .path("/files");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "file_id": "16fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "dataset_id": "26fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    // We don't actually want to try to download from cloud
                    // storage, so we'll force the overwrite prompt by matching
                    // filename of test config file and respond with no.
                    "url": "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/26fb2ac2-642a-4d7e-8233-b1835623b46b/fixtures/test_full_config.toml",
                    "filesize": 123,
                    "version": "blah",
                    "metadata": {},
                }, {
                    "file_id": "16fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "dataset_id": "26fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    "url": "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/26fb2ac2-642a-4d7e-8233-b1835623b46b/fixtures/someotherfile.dat",
                    "filesize": 123,
                    "version": "blah",
                    "metadata": {},
                }]));
        });

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("download")
            .arg("26fb2ac2-642a-4d7e-8233-b1835623b46b")
            .env("BOLSTER__DATABASE__URL", server.base_url())
            .write_stdin("n")
            .assert()
            .success()
            .stdout(predicate::str::contains("Downloading 2 files, total 246 B"))
            .stdout(predicate::str::contains(
                "Overwrite file: fixtures/test_full_config.toml ? [y/n]",
            ));
        mock.assert();
    }

    #[test]
    fn test_cli_download_prefixes_changes_query_params() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .header("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiODA3Y2ZmZTUtZGY2ZC00MzRhLTg2YTQtZDAwN2NkNzQ2YmQzIn0.761nFCTaAsLnU-VaUrLDMNKL6VffxEL9acYbYIaT7tQ")
                .query_param("dataset_id", "eq.26fb2ac2-642a-4d7e-8233-b1835623b46b")
                .query_param("or", "(filepath.ilike.test_full*)")
                .path("/files");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(json!([{
                    "file_id": "16fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "dataset_id": "26fb2ac2-642a-4d7e-8233-b1835623b46b",
                    "created_date": "2021-02-03T21:21:57.713584+00:00",
                    // We don't actually want to try to download from cloud
                    // storage, so we'll force the overwrite prompt by matching
                    // filename of test config file and respond with no.
                    "url": "https://tangram-vision-datasets.s3.us-west-1.amazonaws.com/26fb2ac2-642a-4d7e-8233-b1835623b46b/fixtures/test_full_config.toml",
                    "filesize": 123,
                    "version": "blah",
                    "metadata": {},
                }]));
        });

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("download")
            .arg("26fb2ac2-642a-4d7e-8233-b1835623b46b")
            .arg("test_full")
            .env("BOLSTER__DATABASE__URL", server.base_url())
            .write_stdin("n")
            .assert()
            .success()
            .stdout(predicate::str::contains("Downloading 1 files, total 123 B"))
            .stdout(predicate::str::contains(
                "Overwrite file: fixtures/test_full_config.toml ? [y/n]",
            ));
        mock.assert();
    }

    #[test]
    fn test_cli_digitalocean_provider_available() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg("--provider=digitalocean")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg("non-existent-file")
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                r#"Data file ("non-existent-file") does not exist or is unreadable"#,
            ));
    }

    #[test]
    fn test_cli_plex_file_must_exist() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/non-existent.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("fixtures/empty.bag");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(format!(
                "Plex file ({:?}) does not exist or is unreadable",
                plex_filepath
            )));
    }

    #[test]
    fn test_cli_errors_if_plex_path_has_dots() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/../fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("fixtures/empty.bag");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Paths must not contain './' or '../'.",
            ));
    }

    #[test]
    fn test_cli_errors_if_data_path_has_dots() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("../bolster/fixtures/empty.bag");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Paths must not contain './' or '../'.",
            ));
    }

    #[test]
    fn test_cli_errors_if_uploading_too_many_files() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/empty.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("target");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Please tar/zip the files before uploading!",
            ));
    }

    #[test]
    fn test_cli_errors_on_bad_object_space_toml() {
        let mut cmd = Command::cargo_bin("bolster").expect("Calling binary failed");
        let plex_filepath = Path::new("fixtures/example.plex");
        let toml_filepath = Path::new("fixtures/empty.toml");
        let filepath = Path::new("fixtures/empty.bag");

        cmd.arg("--config")
            .arg("fixtures/test_full_config.toml")
            .arg("upload")
            .arg("robot-01")
            .arg(plex_filepath)
            .arg(toml_filepath)
            .arg(filepath)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Unable to read TOML object-space file!",
            ));
    }
}
