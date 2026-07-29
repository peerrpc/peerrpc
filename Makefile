BUF ?= buf
GO   ?= go

.PHONY: all lint generate gen-vectors test-vectors check-go tidy build-peerrpc

all: lint generate gen-vectors test-vectors

lint:
	$(BUF) lint

generate:
	$(BUF) generate

gen-vectors:
	cd peerrpc-go && $(GO) run ./cmd/gen-vectors

test-vectors:
	cd peerrpc-go && $(GO) test ./protocol/...

check-go:
	cd peerrpc-go && $(GO) build ./... && $(GO) vet ./...

build-peerrpc:
	cd cmd/peerrpc && $(GO) build -o ../../peerrpc .

tidy:
	cd peerrpc-go && $(GO) mod tidy
	cd cmd/peerrpc && $(GO) mod tidy
