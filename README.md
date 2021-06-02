# Bolster

CLI tool for managing datasets, including upload/download and
search/sort/filter.

---

[![pipeline status](https://gitlab.com/tangram-vision-oss/bolster/badges/main/pipeline.svg)](https://gitlab.com/tangram-vision-oss/bolster/-/commits/main)

[![coverage report](https://gitlab.com/tangram-vision-oss/bolster/badges/main/coverage.svg)](https://gitlab.com/tangram-vision-oss/bolster/-/commits/main)

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

For more detailed usage documentation, see: https://tangram-vision-oss.gitlab.io/bolster/bolster/

# Configuration

The configuration file is expected at `~/.config/tangram_vision/bolster.toml` by
default. If you want to run bolster against a particular environment (e.g. dev,
prod), change the contents of the config file, or have multiple config files and
switch between them with the `-c`/`--config` CLI option.

# Contributing

See our [contributing guidelines](CONTRIBUTING.md) before starting any work in
the repository.
