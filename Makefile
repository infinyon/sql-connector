integration_tests:
	cdk build -p sql-sink
	smdk build -p json-sql
	smdk load -p json-sql
	RUST_LOG=warn,integration_tests=info cargo run --release -p integration-tests

