# Quick reference for common Phase-0+ tasks. Prefer running the commands
# directly; this Makefile exists so contributors do not have to memorize
# the exact paths.

BUF ?= buf
GO   ?= go

.PHONY: all lint generate gen-vectors test-vectors check-go tidy

all: lint generate gen-vectors test-vectors

lint:
	$(BUF) lint

generate:
	$(BUF) generate

# Regenerate the golden .bin files + expected.json. Commit the result.
gen-vectors:
	cd peerrpc-go && $(GO) run ./cmd/gen-vectors

# Run the Phase-0 regression test (Go reference).
test-vectors:
	cd peerrpc-go && $(GO) test ./protocol/...

check-go:
	cd peerrpc-go && $(GO) build ./... && $(GO) vet ./...

tidy:
	cd peerrpc-go && $(GO) mod tidy
