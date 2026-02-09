# Contributing to automerge-go

## Prerequisites
- Go 1.24+
- `golangci-lint` v2+

Install lint tool:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

## Local Development
From `automerge-go/`:

```bash
make fmt
make fmt-check
make vet
make test
make lint
```

Run all checks:

```bash
make check
```

## Test Groups
- Unit tests:

```bash
go test ./automerge ./internal/...
```

- Compatibility tests:

```bash
go test ./compat/interop -v
```
