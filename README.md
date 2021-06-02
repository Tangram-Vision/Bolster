# Bolster

CLI tool for managing datasets, including upload/download and
search/sort/filter.

---

[![pipeline status](https://gitlab.com/tangram-vision/bolster/badges/main/pipeline.svg)](https://gitlab.com/tangram-vision/bolster/-/commits/main)

[![coverage report](https://gitlab.com/tangram-vision/bolster/badges/main/coverage.svg)](https://gitlab.com/tangram-vision/bolster/-/commits/main)

---


# Usage

For CLI usage, run the command and use the `help` subcommand or `--help` flag, e.g.:

```
cargo run -- help
cargo run -- help ls
cargo run -- ls --help
```

To install the `bolster` app locally, do:

```
cargo install --path .
```

Run tests with:

```
cargo test
```

To test interaction with the Datasets API you must run a local version of
that API ([see corresponding
README](https://gitlab.com/tangram-vision/devops/-/tree/greg/postgrest-app/tangram-datasets))
and make sure your configuration file has the `database.url` pointed at the
local API endpoint (e.g. set `url = "http://0.0.0.0:3000"`).

# Configuration

The configuration file is expected at `~/.config/tangram_vision/bolster.toml` by
default. If you want to run bolster against a particular environment (e.g. dev,
prod), change the contents of the config file, or have multiple config files and
switch between them with the `-c`/`--config` CLI option.

# Documentation

This tool is currently WIP. For design documentation, see
https://www.notion.so/tangramvision/TANG-upload-download-search-2bd13054bc474bc7a669af90e92584ba?d=9bb9b14b-3813-498b-8247-372d6ad03cba#c6f91c59096f40aab3159b503ba9c595

This tool was based on https://github.com/rust-starter/rust-starter

# Contributing

See our [contributing guidelines](CONTRIBUTING.md) before starting any work in
the repository.
