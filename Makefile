run:
	@mkdir -p target/bin
	@cargo build --target x86_64-unknown-linux-musl
	@cp target/x86_64-unknown-linux-musl/debug/kv-server target/bin
	@cp target/x86_64-unknown-linux-musl/debug/kv-client target/bin
	@docker build -t kv .
	@docker run -it --rm kv
