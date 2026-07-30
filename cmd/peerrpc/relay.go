package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/peerrpc/go/relay"
	signalsdk "github.com/peerrpc/go/signal"
	"github.com/spf13/cobra"
)

var relayCmd = &cobra.Command{
	Use:   "relay",
	Short: "Run the application-layer relay",
	Long: `Start the PeerRPC relay node.

The relay joins a signaling room and forwards WebRTC DataChannel
frames between two peers that cannot reach each other directly.`,
	RunE: runRelay,
}

var relayFlags struct {
	signalAddr string
	service     string
	peerID     string
	bufSize    int
}

func init() {
	f := relayCmd.Flags()
	f.StringVar(&relayFlags.signalAddr, "signal", "", "signal server base URL")
	f.StringVar(&relayFlags.service, "service", "", "service id to relay between two peers")
	f.StringVar(&relayFlags.peerID, "peer-id", "relay", "peer id for the signaling room")
	f.IntVar(&relayFlags.bufSize, "buf", 256, "per-direction forward buffer (frames)")
	_ = relayCmd.MarkFlagRequired("service")
}

func runRelay(_ *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var backend signalsdk.Backend
	if relayFlags.signalAddr != "" {
		backend = signalsdk.NewWS(relayFlags.signalAddr)
		logger.Info("using remote signaling", "addr", relayFlags.signalAddr)
	} else {
		backend = signalsdk.NewLocal()
		logger.Info("using in-process signaling")
	}

	srv, err := relay.New(relay.Config{
		Signaling:         backend,
		ForwardBufferSize: relayFlags.bufSize,
		Logger:            logger,
	})
	if err != nil {
		logger.Error("relay.New failed", "err", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("relay node starting", "service", relayFlags.service, "peer_id", relayFlags.peerID)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx, relayFlags.service, relayFlags.peerID) }()

	<-ctx.Done()
	logger.Info("relay node shutting down")
	_ = srv.Close()
	return nil
}
