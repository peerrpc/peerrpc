package grpcbridge_test

import (
	"strings"
	"testing"

	"github.com/peerrpc/go/grpcbridge"
)

func TestParseServiceSpec(t *testing.T) {
	cases := []struct {
		in        string
		name      string
		methods   []string
		wantError bool
	}{
		{"echo.Echo:Echo,Stream", "echo.Echo", []string{"Echo", "Stream"}, false},
		{"foo.Bar:Get", "foo.Bar", []string{"Get"}, false},
		{"foo.Bar: Get , Put ", "foo.Bar", []string{"Get", "Put"}, false},
		{"", "", nil, true},
		{"Missing", "", nil, true},
		{":NoName", "", nil, true},
		{"NoMethods:", "", nil, true},
	}
	for _, c := range cases {
		name, methods, err := grpcbridge.ParseServiceSpec(c.in)
		if c.wantError {
			if err == nil {
				t.Errorf("ParseServiceSpec(%q): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseServiceSpec(%q): %v", c.in, err)
			continue
		}
		if name != c.name {
			t.Errorf("name: got %q want %q", name, c.name)
		}
		if strings.Join(methods, ",") != strings.Join(c.methods, ",") {
			t.Errorf("methods: got %v want %v", methods, c.methods)
		}
	}
}
