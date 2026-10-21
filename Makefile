.PHONY: test integration build check
test:
	cargo test --locked --lib
integration:
	cargo test --locked
build:
	cargo build --release --locked
check:
	cargo fmt --check
	cargo clippy --locked --all-targets -- -D warnings
