# Default paths to binaries
BINARIES_PATH?=
FLUVIO_BIN?=$(BINARIES_PATH)fluvio
SMDK_BIN?=$(BINARIES_PATH)smdk
CDK_BIN?=$(BINARIES_PATH)cdk
START_FLUVIO?=true

# Integration tests target
integration_tests:
	$(CDK_BIN) build -p sql-sink
	$(SMDK_BIN) build -p json-sql
	FLUVIO_BIN=$(FLUVIO_BIN) CDK_BIN=$(CDK_BIN) SMDK_BIN=$(SMDK_BIN) \
		   START_FLUVIO=$(START_FLUVIO) RUST_LOG=warn,integration_tests=info \
		   cargo run --release -p integration-tests
