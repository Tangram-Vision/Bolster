Make sure documentation is updated:

1. Review docs: `cargo doc --open --no-deps`
2. Capture new screenshots if needed
3. Record new gifs if needed, using Peek
    1. Disable shell overrides, e.g. `starship`
    2. Use a plain shell prompt: `export PS1="\$ "`
    3. Set shell size to 100x24
    4. Set peek size to 1000x457
    5. Put resulting gifs in `assets/`
4. Update text content, particularly `lib.rs` rustdocs



To make a release:

1. Create a new branch off `main`, e.g. `release/0.2`
2. (Optional) update dependencies: `cargo update && git commit -m "Update deps"`
    1. NOTE: We vendor openssl (for musl builds), so make sure to keep it up-to-date for security
3. Increment version in `Cargo.toml`
4. Create a new commit, e.g. `git commit -m "0.2.0"`
5. Create a new tag, e.g. `git tag -a -m "0.2.0" release/0.2.0`
6. Push everything: `git push -u origin --follow-tags HEAD`
7. Build a binary release: `cargo build --release`
8. Build a musl binary release: `cargo build --release --target x86_64-unknown-linux-musl`
    1. You must have added the target first: `rustup target add x86_64-unknown-linux-musl`
    2. To build openssl source: `sudo apt install musl-tools`
9. Strip binaries to reduce size: `strip path/to/binary`
10. Tar and gzip binaries, e.g. `tar -czvf bolster.x86_64-unknown-linux-gnu.tar.gz bolster`
11. Create a new release: https://gitlab.com/tangram-vision-oss/bolster/-/releases
    1. Select the tag (e.g. `0.2.0`)
    2. Set the title
    3. Add release notes
    4. Attach binary files
    5. Add a link to documentation
12. Test installing by following documentation instructions