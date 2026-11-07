package manager_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/relux-works/swarma-session-host/manager"
)

// An external composition can use the public seam without importing internal
// packages. Invalid finite custody refuses before creating state or a runner;
// zero values do not grant a usable manager or panic during status/close.
func TestPublicManagerRefusesMissingLifetime(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	m, err := manager.Open(context.Background(), manager.Options{StateDir: state})
	if m != nil || err == nil {
		t.Fatal("missing lifetime admitted")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("invalid custody created state", err)
	}
	var zero manager.Manager
	if _, err := zero.Snapshot(context.Background()); err == nil {
		t.Fatal("zero manager granted status")
	}
	if a, err := zero.Prepare(context.Background(), "launch", []byte(`{}`)); a != nil || err == nil {
		t.Fatal("zero manager admitted operation")
	}
	if err := zero.Close(context.Background()); err == nil {
		t.Fatal("zero manager closed as admitted")
	}
	var attachment manager.Attachment
	if _, err := attachment.Projection(); err == nil {
		t.Fatal("zero attachment granted projection")
	}
	if err := attachment.Bootstrap(context.Background()); err == nil {
		t.Fatal("zero attachment bootstrapped")
	}
	if err := attachment.Close(); err != nil {
		t.Fatal(err)
	}
}
