.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=tpch_rust

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.3

# Pin the Python duckdb used by the test runner to the target version. The unstable C ABI means
# the test client must match TARGET_DUCKDB_VERSION exactly, so don't rely on PyPI's "latest".
DUCKDB_TEST_VERSION=1.5.3

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug:
	$(PYTHON_VENV_BIN) -m duckdb_sqllogictest --test-dir ./test/sql --external-extension ./build/debug/extension/tpch_rust/tpch_rust.duckdb_extension --file-path test/sql/tpch_rust.test
test_release:
	$(PYTHON_VENV_BIN) -m duckdb_sqllogictest --test-dir ./test/sql --external-extension ./build/release/extension/tpch_rust/tpch_rust.duckdb_extension --file-path test/sql/tpch_rust.test

clean: clean_build clean_rust
clean_all: clean_configure clean
