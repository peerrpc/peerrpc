// Command peerrpc-relay runs the v1 PeerRPC application-layer relay.
//
// The relay joins a signaling room and accepts DataChannels from
// peers that cannot reach each other directly (symmetric NAT,
// restrictive firewalls, no common TURN). It forwards bytes verbatim
// between the two DataChannels; a relayed RPC is bit-identical to a
// P2P RPC.
//
// v1 limitations (PLAN.md §4.2):
//   * Single node. No mesh routing, no auto-discovery, no failover.
//   * Static configuration. The room_id is given on the command line.
//   * One room per invocation. v1.1 will multiplex.
//
// Usage:
//
//	peerrpc-relay -signal http://localhost:8080 \
//	              -room "room-id-from-app" \
//	              -peer-id "relay-1"
//
// Without -signal, the relay uses the in-process signaling backend
// suitable for localhost demos. Pass -signal to point at a standalone
// signal-server instead.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/peerrpc/go/relay"
	signalsdk "github.com/peerrpc/go/signal"

	"log/slog"
)

func main() {
	signalAddr := flag.String("signal", "", "signal server base URL (e.g. http://localhost:8080)")
	roomID := flag.String("room", "", "room id to relay between two peers")
	peerID := flag.String("peer-id", "relay", "peer id the relay uses in the signaling room")
	bufSize := flag.Int("buf", 256, "per-direction forward buffer (frames)")
	flag.Parse()

	if *roomID == "" {
		flag.Usage()
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Select signaling backend: remote if -signal is given, else in-process.
	var backend signalsdk.Backend
	if *signalAddr != "" {
		backend = signalsdk.NewWS(*signalAddr)
		logger.Info("using remote signaling", "addr", *signalAddr)
	} else {
		backend = signalsdk.NewLocal()
		logger.Info("using in-process signaling")
	}

	srv, err := relay.New(relay.Config{
		Signaling:         backend,
		ForwardBufferSize: *bufSize,
		Logger:            logger,
	})
	if err != nil {
		logger.Error("relay.New failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("relay node starting", "room_id", *roomID, "peer_id", *peerID)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx, *roomID, *peerID) }()

	select {
	case err := <-errCh:
		if err != nil && !isContextCanceled(err) {
			logger.Error("relay Serve failed", "err", err)
			os.Exit(1)
		}
	case <-time.After(0): // immediate; serve is blocking
	}

	<-ctx.Done()
	logger.Info("relay node shutting down")
	_ = srv.Close()
}

func isContextCanceled(err error) bool {
	if err == nil {
		return false
	}
	return err == context.Canceled || err.Error() == context.Canceled.Error()
}
