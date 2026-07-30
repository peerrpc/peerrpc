BUF  ?= buf
GO   ?= go
NPM  ?= npm
CARGO ?= cargo

.PHONY: all lint generate gen-vectors test-vectors check-go tidy build-peerrpc build-peerrpc-interop-ts
.PHONY: build-ts build-rs
.PHONY: test-all test-go test-rs test-ts

# Quick-start targets (signal-server + echo server + browser client).
.PHONY: run-signal run-echo run-echo-server run-echo-server-ts run-echo-ts run-echo-react

# Local (single-process, no signal-server) demos.
.PHONY: run-local-echo-go run-local-echo-rs run-local-facade-go run-local-facade-rs run-local-facades

# Cross-language interop samples (test/cross-lang/).
.PHONY: run-interop-server run-interop-rs run-interop-e2e

all: lint generate gen-vectors test-vectors

lint:
	$(BUF) lint

generate:
	$(BUF) generate

gen-vectors:
	cd peerrpc-go && $(GO) run ./cmd/gen-vectors

test-vectors:
	cd peerrpc-go && $(GO) test ./protocol/...

# ── All-language test suite ────────────────────────────────────────
#
# Runs every SDK and server test suite. Each sub-target is independent
# so you can run a single language:
#   make test-go
#   make test-rs
#   make test-ts

test-all: test-go test-rs test-ts

# Go: the main SDK (with -race) + the standalone server modules.
# Examples are demo binaries with no tests, so they are skipped.
GO_TEST_DIRS := peerrpc-go signal-server relay-server grpcbridge-server cmd/peerrpc

test-go:
	@for dir in $(GO_TEST_DIRS); do \
		echo "=== go test $$dir ==="; \
		(cd $$dir && $(GO) test ./... -race -count=1 -timeout 180s) || exit 1; \
	done

# Rust: the SDK workspace, then the standalone example crates.
test-rs:
	$(CARGO) test --manifest-path peerrpc-rs/Cargo.toml --workspace
	cd examples/rs/echo && $(CARGO) test
	cd examples/rs/facade && $(CARGO) test

# TypeScript: vitest discovers and runs every *.test.ts.
test-ts:
	$(NPM) --prefix peerrpc-ts install >/dev/null
	$(NPM) --prefix peerrpc-ts run test

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

# ── Quick start: signal-server + echo server + browser client ───
#
# Run each in a separate terminal:
#   make run-signal       # signal-server (WebSocket; cleartext ws://)
#   make run-echo-server  # Go echo RPC server (all 4 types)
#   make run-echo-ts      # Vite dev server for the browser echo page
#
# Then open the printed Vite URL and click "Connect". For a TLS setup
# (wss://) pass SIGNAL_TLS=1 and accept the self-signed cert first.

SIGNAL_ADDR ?= :8443
SIGNAL_TLS  ?= 0
ECHO_PORT   ?= 5173

run-signal:
	cd cmd/peerrpc && $(GO) run . signal --addr $(SIGNAL_ADDR) $(if $(filter 1,$(SIGNAL_TLS)),--auto-tls)

run-echo-server:
	cd examples/go/echo-server && $(GO) run .

run-echo-ts:
	cd examples/ts/echo && $(NPM) install && $(NPM) run dev -- --port $(ECHO_PORT)

run-echo-react:
	cd examples/ts/echo-react && $(NPM) install && $(NPM) run dev -- --port $(ECHO_PORT)

run-echo-server-ts:
	cd examples/ts/echo-server && $(NPM) install && $(NPM) run dev -- --port $(ECHO_PORT)

# Convenience: prints instructions for the three-terminal quick start.
run-echo:
	@echo "Run each in a separate terminal:"
	@echo "  make run-signal"
	@echo "  make run-echo-server   (Go)  or  make run-echo-server-ts  (browser)"
	@echo "  make run-echo-ts       (vanilla TS)  or  make run-echo-react  (React)"
	@echo ""
	@echo "Then open the Vite URL and click Connect. No TLS setup needed"
	@echo "(cleartext by default; use SIGNAL_TLS=1 for wss://)."

# ── Per-language echo demos (local signaling, no server needed) ───

run-local-echo-go:
	cd examples/go/echo && $(GO) run .

run-local-echo-rs:
	$(CARGO) run --manifest-path examples/rs/echo/Cargo.toml

# ── Facade examples (each language, local-only signaling) ────────

run-local-facade-go:
	cd examples/go/facade && $(GO) run .

run-local-facade-rs: build-rs
	$(CARGO) run --manifest-path examples/rs/facade/Cargo.toml

run-local-facades:
	@echo "Run each facade in a separate terminal:"
	@echo "  make run-local-facade-go"
	@echo "  make run-local-facade-rs"

# ── Cross-language interop samples ──────────────────────────────
#
# The Go interop server serves all four RPC types (Unary / Stream /
# Collect / Chat) and provides SSE signaling for a browser or native
# client. Start it, then run a client in another terminal.

STATIC_DIR ?=

run-interop-server:
	cd test/cross-lang/go-ts && $(GO) run . -addr :30443 -auto-tls $(if $(STATIC_DIR),-static $(abspath $(STATIC_DIR)))

run-interop-rs:
	@echo "Start the interop server first:"
	@echo "  make run-interop-server"
	@echo ""
	cd test/cross-lang/go-rs && $(CARGO) run -- http://localhost:30443

run-interop-e2e:
	@echo "Start the interop server first:"
	@echo "  make run-interop-server"
	@echo ""
	cd test/cross-lang/go-ts/e2e && $(NPM) install && npx playwright test
