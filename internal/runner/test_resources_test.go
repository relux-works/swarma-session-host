package runner

import (
	"encoding/json"
	"os"
	"syscall"
	"testing"
)

// resourceReceipt records only this test's owned PID/path, never process argv or
// environment. Optional lane receipts let the outer runner independently audit
// these specific resources even when a failing test still runs its cleanup.
func resourceReceipt(t *testing.T, kind string, pid int, path string) {
	t.Helper()
	receipt := struct {
		Kind string `json:"kind"`
		PID  int    `json:"pid,omitempty"`
		Path string `json:"path,omitempty"`
	}{kind, pid, path}
	data, _ := json.Marshal(receipt)
	t.Logf("owned_resource %s", data)
	if ledger := os.Getenv("SESSION_HOST_TEST_RESOURCES"); ledger != "" {
		f, err := os.OpenFile(ledger, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(data, '\n'))
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("resource receipt: %v %v", err, closeErr)
		}
	}
}

func startTestRunner(t *testing.T, cfg Config) (*Server, error) {
	t.Helper()
	s, err := Start(cfg)
	if err == nil {
		trackTestRunner(t, s)
	}
	return s, err
}

func trackTestRunner(t *testing.T, s *Server) {
	t.Helper()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("owned child cleanup: %v", err)
		}
		if err := syscall.Kill(s.pid, 0); err != syscall.ESRCH {
			t.Errorf("OWNED_CHILD_LEFTOVER: pid=%d probe=%v", s.pid, err)
		}
		resourceReceipt(t, "child-end", s.pid, "")
		if _, err := os.Lstat(s.cfg.Socket); !os.IsNotExist(err) {
			t.Errorf("OWNED_SOCKET_LEFTOVER: %s %v", s.cfg.Socket, err)
		}
	})
	resourceReceipt(t, "child-start", s.pid, "")
	resourceReceipt(t, "socket", 0, s.cfg.Socket)
}
