Bolster is a Command Line Interface (CLI) from Tangram Vision for managing
sensor datasets and results of processing them.

Bolster allows you to upload, download, and query datasets. Uploading
datasets will trigger processing in the cloud using our computer vision
algorithms. Processed results will be delivered via email and will also be
available for download via bolster.

# Installation

If you have [Rust installed](https://rustup.rs/), you can install bolster
with:

```shell
cargo install --branch=release/0.3 --git=https://gitlab.com/tangram-vision-oss/bolster.git
```

Alternatively, release binaries are published for supported platforms at
<https://gitlab.com/tangram-vision-oss/bolster/-/releases/>.

# Usage

View CLI help with `bolster help` or `bolster help <subcommand>`.

Bolster is intended to be used as a binary -- if you want to use it as a
library, [email us about your use case](mailto:feedback@tangramvision.com)!

## Configuration

Bolster requires a configuration file to successfully interact with web
services. A configuration file is provided to you when you join the Alpha
program. To use the configuration file with bolster, either:

- Place the configuration file at `~/.config/tangram_vision/bolster.toml`
- Or use the `--config path/to/bolster.toml` flag

## Commands

```bolster config```

Echoes current config (with any overrides applied) and exits.

<br>

---

```bolster upload <SYSTEM_ID> <PLEX_PATH> <OBJECT_SPACE_TOML_PATH> <PATH>...```

Creates a new dataset associated with the system ID and uploads all
files in the provided path(s). If any data path (the last argument, which
may be repeated) is a directory, all files in the directory will be
uploaded. Folder structure is preserved when uploading to cloud storage.
Does not follow symlinks.

Uploading files creates a new dataset and outputs the created dataset's
UUID, which can be used to download or query the dataset or the files it
contains in the future.

The `<SYSTEM_ID>` provided when uploading a dataset should match however
you identify your systems/robots/installations, whether that be by an
integer (e.g. "unit 1") or a serial (e.g. "A12") or a build date (e.g.
"12-MAY-2021") or a location (e.g. "field3" or "southwest-corner") or
anything else. The dataset will be associated with the given system_id, to
allow filtering datasets (and processing results) by system.

For more info about plexes and object-space TOML files, please see the
Tangram Vision SDK documentation.

Note: Only files up to 4.88 TB may be uploaded.

When uploading a dataset, filenames must be valid UTF-8 (this is a
requirement of cloud storage providers such as [AWS
S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)).

![Bolster upload example
gif](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-upload-0.3.0.gif)

<br>

---

```bolster download <DATASET_UUID> [PREFIX]...```

Downloads files from the given dataset. Files to download may be filtered
by providing prefix(es). If multiple prefixes are provided, all files
matching any prefix will be downloaded.

If downloading a file would overwrite an existing file, the user is
prompted to continue.

![Bolster download example
gif](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-download-0.3.0.gif)

<br>

---

```bolster ls [OPTIONS]```

List all datasets associated with your account. Datasets may be filtered
or sorted using various options (e.g. by creation date). If a specific
dataset is selected with the `--uuid` option, files in that dataset will be
listed.

![Bolster ls example
image](https://tangram-vision-oss.gitlab.io/bolster/assets/bolster-ls-0.3.0.png)

## Examples

```shell
##################
# bolster upload
##################

# Uploads the "robot-walle" system's plex and object-space TOML, along with
# ros-data.bag as a new dataset.
bolster upload robot-walle v1.plex checkerboard.toml ros-data.bag

# Uploads contents of the data folder as a new dataset for the
# "drone-maverick" system.
bolster upload drone-maverick maverick.plex object-space.toml data/*

# Uploads all files in camera-1 and camera-2 folders as a new dataset for
# "johnny-5" system.
bolster upload johnny-5 2021aug.plex 2021aug.toml camera-1 camera-2

####################
# bolster download
####################

# Downloads all files in dataset 1415fe36-851f-4c62-a616-4f5e343ba5fc to
# your current working directory.
bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc

# Downloads files myfile1 and myfile2 from the dataset to your current
# working directory.
bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc myfile1 myfile2

# Downloads all files in myfolder1 of the remote dataset into myfolder1 in
# your current working directory. Creates myfolder1 if it does not exist.
bolster download 1415fe36-851f-4c62-a616-4f5e343ba5fc myfolder1

##############
# bolster ls
##############

# List 100 datasets instead of showing the default limit of 20
bolster ls --limit=100

# List all files in the specified dataset
bolster ls --uuid=1415fe36-851f-4c62-a616-4f5e343ba5fc

# List datasets created in 2021 and sort them most-recent-first
bolster ls --after-date 2021-01-01 --order-by=created_date.desc
```

# Troubleshooting

If you're encountering issues using bolster, please refer to the table below
for potential solutions. If the issue persists, please [email
us](mailto:feedback@tangramvision.com).

| Error | Resolution |
|-|-|
| Configuration file not found | Bolster will use a configuration file located at `~/.config/tangram_vision/bolster.toml` by default. Alternately, provide a config file via the `--config` option, e.g. `bolster --config=path/to/bolster.toml ls`. |
| Connection refused | Bolster upload/download/ls subcommands require an internet connection -- make sure your connection is working and that you can reach bolster.tangramvision.com and s3.us-west-1.amazonaws.com without interference or disruption from any firewalls or proxies. |
| All file/folder names must be valid UTF-8 | All filepaths uploaded as a dataset must be valid UTF-8 as required by S3-compatible cloud storage providers. |
| File/folder paths must be relative | You may not use absolute filepaths with the upload sub-command, such as `/dir/file` or `~/dir/file`, because bolster preserves the folder structure of uploaded files. |

# Security

Bolster connects to web services using TLS (data is encrypted in transit).
Datasets stored with cloud storage providers are encrypted at rest. All
public access is blocked to the cloud storage bucket. Only the credentials
in the bolster config file provided to you (and a restricted set of Tangram
Vision processes and employees) have access to your datasets when uploaded
to cloud storage.

If you would like to report a security bug or issue, email *support ~@~
tangramvision.com*. Our team will acknowledge your email within 72 hours,
and will send a more detailed response within 72 hours indicating the next
steps in handling your report. After the initial reply to your report, we
will endeavor to keep you informed of the progress towards a fix and full
announcement, and may ask for additional information or guidance.

# Performance

Bolster currently uploads up to 4 files in parallel with each file uploading
up to 10 separate 16-MB chunks at a time. So, bolster may use up to 640 MB
of RAM (plus some overhead). If you're working with a more constrained
environment, please [email us](mailto:feedback@tangramvision.com).

All uploaded files are md5-checksummed for data integrity. As a result, you
may notice some CPU load while uploading.

# Feedback

As always, if you have any feedback, please email us at
feedback@tangramvision.com!

License: MIT
