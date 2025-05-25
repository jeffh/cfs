# Development Instructions

This repository is written in Go and uses the standard Go toolchain. A
`Makefile` is provided which compiles all binaries under `cmd/` and
runs the test suite.

## Building

Run `make` from the repository root. This will build every command
found in `cmd/` and place the resulting binaries in `./bin`.

```
make
```

For race detection or other flags set `GOARGS`:

```
GOARGS=-race make all test
```

## Testing

Execute the Go unit tests using `make test`:

```
make test
```

## Linting

Before committing any changes, lint the code base with
`golangci-lint`:

```
golangci-lint run
```

Fix any issues the linter reports prior to creating a commit.

