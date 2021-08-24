If you want to see the output of `debug!` logging macros while running tests,
add the following line to the top of the test:

```
let _ = env_logger::try_init();
```

and run the tests with a command line like:

```
RUST_LOG=bolster=debug cargo test [<TEST_NAME>] -- --nocapture
```

If you want to see what dependency crates (e.g. rusoto, httpmock) are doing, run
with a command like:

```
RUST_LOG=debug cargo test [<TEST_NAME>] -- --nocapture
```