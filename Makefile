BUF  ?= buf
GO   ?= go
NPM  ?= npm
CARGO ?= cargo

.PHONY: all lint generate gen-vectors test-vectors check-go tidy build-peerrpc build-peerrpc-interop-ts
.PHONY: build-ts build-rs
.PHONY: run-signal run-ts-echo run-echo-server run-ts-echo-server run-echo
.PHONY: run-echo-go run-facade-go run-facade-ts run-facade-rs run-facades
.PHONY: run-echo-rs
.PHONY: run-interop-server run-interop-rs run-interop-e2e run-sample

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

# ── Quick start: signal-server + echo server + browser client ───
#
# Run each in a separate terminal:
#   make run-signal       # signal-server (cleartext h2c; no TLS setup)
#   make run-echo-server  # Go echo RPC server (all 4 types)
#   make run-ts-echo      # Vite dev server for the browser echo page
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

run-ts-echo:
	cd examples/ts/echo && $(NPM) install && $(NPM) run dev -- --port $(ECHO_PORT)

run-ts-echo-server:
	cd examples/ts/echo-server && $(NPM) install && $(NPM) run dev -- --port $(ECHO_PORT)

# Convenience: prints instructions for the three-terminal quick start.
run-echo:
	@echo "Run each in a separate terminal:"
	@echo "  make run-signal"
	@echo "  make run-echo-server   (Go)  or  make run-ts-echo-server  (browser)"
	@echo "  make run-ts-echo"
	@echo ""
	@echo "Then open the Vite URL and click Connect. No TLS setup needed"
	@echo "(cleartext by default; use SIGNAL_TLS=1 for wss://)."

# ── Per-language echo demos (local signaling, no server needed) ───

run-echo-go:
	$(GO) run ./examples/go/echo

run-echo-rs:
	$(CARGO) run --manifest-path examples/rs/echo/Cargo.toml

# ── Facade examples (each language, local-only signaling) ────────

run-facade-go:
	$(GO) run ./examples/go/facade

run-facade-ts: build-ts
	$(NPM) --prefix examples/ts/facade install
	$(NPM) --prefix examples/ts/facade run dev

run-facade-rs: build-rs
	$(CARGO) run --manifest-path examples/rs/facade/Cargo.toml

run-facades:
	@echo "Run each facade in a separate terminal:"
	@echo "  make run-facade-go"
	@echo "  make run-facade-ts"
	@echo "  make run-facade-rs"

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

run-sample: run-interop-server
