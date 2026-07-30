package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/peerrpc/go/grpcbridge"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	signalsdk "github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
	"github.com/spf13/cobra"
)

var bridgeCmd = &cobra.Command{
	Use:   "bridge",
	Short: "Run the gRPC-Connect bridge",
	Long: `Start the PeerRPC -> Connect/gRPC bridge.

Registers PeerRPC service stubs that forward incoming calls to a
remote Connect (or gRPC) service URL over HTTP.`,
	RunE: runBridge,
}

var bridgeFlags struct {
	upstream       string
	signalAddr     string
	room           string
	peerID         string
	role           string
	services       []string
	streamServices []string
}

func init() {
	f := bridgeCmd.Flags()
	f.StringVar(&bridgeFlags.upstream, "upstream", "", "Connect service base URL")
	f.StringVar(&bridgeFlags.signalAddr, "signal", "", "signal server base URL")
	// Signaling room id. Named -room (not -service) to avoid colliding
	// with the repeatable -service flag below.
	f.StringVar(&bridgeFlags.room, "room", "", "signaling room id")
	f.StringVar(&bridgeFlags.peerID, "peer-id", "bridge", "peer id")
	f.StringVar(&bridgeFlags.role, "role", "answerer", "peer role (offerer|answerer)")
	f.StringArrayVar(&bridgeFlags.services, "service", nil, `unary service spec "Name:Method1,Method2"`)
	f.StringArrayVar(&bridgeFlags.streamServices, "stream-service", nil, `server-streaming service spec "Name:Method1,Method2"`)
	_ = bridgeCmd.MarkFlagRequired("upstream")
	_ = bridgeCmd.MarkFlagRequired("room")
}

func runBridge(_ *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	invoker := &grpcbridge.HTTPClientInvoker{
		Base:   bridgeFlags.upstream,
		Client: &http.Client{Timeout: 30 * time.Second},
	}

	peerrpcSrv := rpc.NewServer()

	for _, spec := range bridgeFlags.services {
		name, methods, err := grpcbridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad --service spec", "spec", spec, "err", err)
			return err
		}
		grpcbridge.MountConnectService(peerrpcSrv, name, methods, invoker)
		logger.Info("mounted unary service", "name", name, "methods", methods)
	}

	for _, spec := range bridgeFlags.streamServices {
		name, methods, err := grpcbridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad --stream-service spec", "spec", spec, "err", err)
			return err
		}
		grpcbridge.MountConnectServiceWithStreaming(
			peerrpcSrv, name, methods, methods,
			invoker, invoker,
		)
		logger.Info("mounted streaming service", "name", name, "methods", methods)
	}

	var backend signalsdk.Backend
	if bridgeFlags.signalAddr != "" {
		backend = signalsdk.NewWS(bridgeFlags.signalAddr)
		logger.Info("using remote signaling", "addr", bridgeFlags.signalAddr)
	} else {
		backend = signalsdk.NewLocal()
		logger.Info("using in-process signaling")
	}

	sig, err := backend.Exchange(context.Background(), bridgeFlags.room, bridgeFlags.peerID)
	if err != nil {
		logger.Error("signaling exchange", "err", err)
		return err
	}
	defer sig.Close()

	r, err := grpcbridge.ParseRole(bridgeFlags.role)
	if err != nil {
		logger.Error("bad role", "err", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p, err := peer.New(ctx, r, peer.Config{})
	if err != nil {
		logger.Error("peer.New", "err", err)
		return err
	}
	defer p.Close()

	logger.Info("bridge starting",
		"upstream", bridgeFlags.upstream,
		"room_id", bridgeFlags.room,
		"peer_id", bridgeFlags.peerID,
		"role", r,
	)

	var ch *transport.Channel
	if r == signalsdk.RoleClient {
		ch, err = p.Dial(ctx, sig)
		if err != nil {
			logger.Error("Dial", "err", err)
			return err
		}
	} else {
		ch, err = p.Accept(ctx, sig)
		if err != nil {
			logger.Error("Accept", "err", err)
			return err
		}
	}

	peerrpcSrv.Serve(ctx, ch)
	return nil
}
