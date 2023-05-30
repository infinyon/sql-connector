integration_tests:
	cdk build -p sql-sink
	RUST_LOG=warn,integration_tests=info cargo run --release -p integration-tests

