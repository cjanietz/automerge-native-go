# automerge-native-go

Native Go implementation of [Automerge](https://automerge.org/), based on the semantics and architecture of the original Rust Automerge implementation.

This project is written in pure Go and **does not use cgo**.

## Status

This repository is under active development and currently targets semantic and interoperability parity with Automerge Rust for core document, merge, storage, sync, and text/mark behavior.

## Why This Project

- Native Go API for Automerge-style collaborative data.
- No Rust FFI bridge required.
- No cgo toolchain/runtime dependency.
- Optimized progressively with benchmark-guided tuning.

## Key Features

- Change graph/DAG tracking with deterministic ordering.
- Map/list/text/counter/mark operations.
- Transaction API + AutoCommit API.
- Apply/merge pipeline with causal queueing.
- Binary storage chunk load/save compatibility paths.
- Sync protocol state/message handling.
- Historical reads and diff/patch support.
- Compatibility harness and benchmark suite.

## Install

```bash
go get github.com/cjanietz/automerge-native-go
```

## Current Usage

The API is still being hardened for pre-`v1` release, so the most accurate usage examples are the tests in:

- `automerge/transaction_test.go`
- `automerge/autocommit_test.go`
- `automerge/sync_test.go`
- `automerge/storage_test.go`

## Repository Layout

- `automerge/`: public API (`Document`, `Transaction`, `AutoCommit`, sync, storage)
- `internal/changegraph`: change DAG and visibility clocks
- `internal/opset`: operation storage and read semantics
- `internal/storage`: chunk parse/encode and compression paths
- `internal/sync`: sync state machine and message structures
- `compat/interop`: compatibility harness and fixture-driven tests

## Development

Run tests:

```bash
go test ./...
```

Run compatibility harness:

```bash
COMPAT_REPORT_PATH=../.docs/compatibility-report.json go test ./compat/interop -v
```

Run benchmarks:

```bash
go test ./automerge -run '^$' -bench 'Benchmark(LoadSave|ApplyMerge|TextSplice|SyncMessageGeneration)$' -benchmem
```

## Compatibility and Performance

- The project includes fixture-based compatibility checks against Rust Automerge test assets.
- Performance work is tracked with benchmark/profiling artifacts in `.docs`.

## Non-Goals (Current)

- cgo/Rust FFI bindings.
- Perfect byte-for-byte parity for every experimental Automerge feature on day one.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
