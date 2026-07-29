BUF  ?= buf
GO   ?= go
NPM  ?= npm
CARGO ?= cargo

.PHONY: all lint generate gen-vectors test-vectors check-go tidy build-peerrpc
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

run-interop-server:
	$(GO) run ./test/cross-lang/go-ts -addr :3000 -auto-tls

run-sample: run-interop-server
