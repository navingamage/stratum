SHELL := /bin/bash
GO ?= go
BIN := bin
PKG := ./...

VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS  := -s -w \
	-X github.com/navingamage/stratum/internal/buildinfo.Version=$(VERSION) \
	-X github.com/navingamage/stratum/internal/buildinfo.Commit=$(COMMIT)

.PHONY: all
all: lint test build

.PHONY: build
build:
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/stratumd ./cmd/stratumd
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/stratum  ./cmd/stratum

.PHONY: test
test:
	$(GO) test -race -count=1 $(PKG)

.PHONY: test-short
test-short:
	$(GO) test -short -count=1 $(PKG)

.PHONY: cover
cover:
	$(GO) test -covermode=atomic -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -n 1

# Fuzz targets are short by default; override FUZZTIME for longer soaks.
FUZZTIME ?= 30s
.PHONY: fuzz
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzChunkRoundTrip -fuzztime $(FUZZTIME) ./internal/chunk
	$(GO) test -run '^$$' -fuzz FuzzChunkDecode    -fuzztime $(FUZZTIME) ./internal/chunk
	$(GO) test -run '^$$' -fuzz FuzzWALRecovery    -fuzztime $(FUZZTIME) ./internal/wal
	$(GO) test -run '^$$' -fuzz FuzzRecordDecode   -fuzztime $(FUZZTIME) ./internal/wal
	$(GO) test -run '^$$' -fuzz FuzzParser         -fuzztime $(FUZZTIME) ./internal/query

.PHONY: bench
bench:
	$(GO) test -run '^$$' -bench . -benchmem $(PKG)

.PHONY: lint
lint:
	$(GO) vet $(PKG)
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt: files need formatting"; exit 1)

.PHONY: clean
clean:
	rm -rf $(BIN) coverage.out *.prof
