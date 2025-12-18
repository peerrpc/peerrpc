module github.com/peerrpc/signal-server

go 1.25.0

require (
	connectrpc.com/connect v1.20.0
	github.com/peerrpc/go v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.50.0
)

require (
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/peerrpc/go => ../peerrpc-go
