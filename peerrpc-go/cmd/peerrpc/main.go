package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "peerrpc",
	Short: "PeerRPC — WebRTC-native RPC framework",
	Long: `PeerRPC is a WebRTC-native RPC framework for browser↔server
and server↔server communication over WebRTC DataChannels.`,
	Run: func(cmd *cobra.Command, _ []string) {
		_ = cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(signalCmd)
	rootCmd.AddCommand(relayCmd)
	rootCmd.AddCommand(bridgeCmd)
}
