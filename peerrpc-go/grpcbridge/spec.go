package grpcbridge

import (
	"errors"
	"fmt"
	"strings"

	"github.com/peerrpc/go/signal"
)

// ParseServiceSpec turns a CLI-style "Name:Method1,Method2" spec
// into its components. It is the parser the peerrpc bridge subcommand
// (and any other caller) uses to turn repeatable --service flags into
// a service name plus its method list.
func ParseServiceSpec(s string) (string, []string, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", nil, errors.New("expected Name:Method1,Method2")
	}
	methods := strings.Split(parts[1], ",")
	for i := range methods {
		methods[i] = strings.TrimSpace(methods[i])
	}
	return parts[0], methods, nil
}

// ParseRole maps a human role name to a signal.Role.
//
//	"offerer"  -> signal.RoleClient (the bridge initiates the offer)
//	"answerer" -> signal.RoleServer (the bridge answers an offer)
//
// Any other value yields an error. It is a small helper kept in this
// package so the bridge subcommand and any future caller share one
// definition of the role vocabulary.
func ParseRole(s string) (signal.Role, error) {
	switch s {
	case "offerer":
		return signal.RoleClient, nil
	case "answerer":
		return signal.RoleServer, nil
	default:
		return 0, fmt.Errorf("unknown role %q (want offerer|answerer)", s)
	}
}
