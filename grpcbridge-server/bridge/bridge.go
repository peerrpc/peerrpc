// Package bridge hosts the standalone grpcbridge-server binary's
// types. The binary itself lives in cmd/server; this package exposes
// the spec parser so callers can reuse it.
package bridge

import (
	"errors"
	"strings"
)

// ParseServiceSpec turns a CLI-style "Name:Method1,Method2" spec
// into its components.
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
