package sessionclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type wireRoundTripper func(*http.Request) (*http.Response, error)

func (f wireRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type wireBody struct {
	io.Reader
	closed bool
}

func (b *wireBody) Close() error { b.closed = true; return nil }

// Malformed local input must stop before any transport effect. A valid control
// in the route matrix exercises the neighboring admitted request.
func TestClientCompatibilityEncodingFailureHasNoRequest(t *testing.T) {
	calls := 0
	c := &Client{HTTPClient: &http.Client{Transport: wireRoundTripper(func(*http.Request) (*http.Response, error) { calls++; return nil, fmt.Errorf("unexpected transport") })}}
	err := c.DoRequest(context.Background(), "POST", "/v1/fixture", func() {}, nil, true)
	if err == nil || !strings.Contains(err.Error(), "encoding manager request") || calls != 0 {
		t.Fatalf("invalid JSON reached transport: calls=%d error=%v", calls, err)
	}
}
