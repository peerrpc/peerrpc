BUF  ?= buf
GO   ?= go
NPM  ?= npm
CARGO ?= cargo

.PHONY: all lint generate gen-vectors test-vectors check-go tidy build-peerrpc build-peerrpc-interop-ts
.PHONY: build-ts build-rs
.PHONY: run-facade-go run-facade-ts run-facade-rs run-facades
.PHONY: run-interop-server run-sample

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

build-peerrpc-interop-ts:
	cd test/cross-lang/go-ts && $(GO) build -o ../../peerrpc-interop-ts .

tidy:
	cd peerrpc-go && $(GO) mod tidy
	cd cmd/peerrpc && $(GO) mod tidy

# ── Build SDKs ────────────────────────────────────────────────────

build-ts:
	$(NPM) --prefix peerrpc-ts install
	$(NPM) --prefix peerrpc-ts run build

build-rs:
	$(CARGO) build --manifest-path peerrpc-rs/Cargo.toml --lib

# ── Facade examples (each language, local-only signaling) ────────

run-facade-go:
	$(GO) run ./examples/go/facade

run-facade-ts: build-ts
	$(NPM) --prefix examples/ts/facade install
	$(NPM) --prefix examples/ts/facade run dev

run-facade-rs: build-rs
	$(CARGO) run --manifest-path peerrpc-rs/Cargo.toml -p peerrpc-facade-demo

run-facades:
	@echo "Run each facade in a separate terminal:"
	@echo "  make run-facade-go"
	@echo "  make run-facade-ts"
	@echo "  make run-facade-rs"

# ── Cross-language samples ───────────────────────────────────────

STATIC_DIR ?=

run-interop-server:
	cd test/cross-lang/go-ts && $(GO) run . -addr :3000 -auto-tls $(if $(STATIC_DIR),-static $(abspath $(STATIC_DIR)))

run-sample: run-interop-server
