# fabric-iroh quality gates.
verify:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test
