package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Regression witnesses for result, filesystem and stop boundaries.
func TestReviewRequiredResultEvidence(t *testing.T) {
	good := resultJSON(fixedSession, false, 0)
	cases := map[string]string{
		"missing_is_error":                strings.Replace(good, `"is_error":false,`, "", 1),
		"null_is_error":                   strings.Replace(good, `"is_error":false`, `"is_error":null`, 1),
		"missing_denials":                 strings.Replace(good, `"permission_denials":[],`, "", 1),
		"null_denials":                    strings.Replace(good, `"permission_denials":[]`, `"permission_denials":null`, 1),
		"duplicate_error_true_then_false": strings.Replace(good, `"is_error":false`, `"is_error":true,"is_error":false`, 1),
		"duplicate_denials_then_empty":    strings.Replace(good, `"permission_denials":[]`, `"permission_denials":[{"tool_name":"Bash"}],"permission_denials":[]`, 1),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			c, out, _, _ := fakeChild(t, 0, payload)
			err := c.handle(noticeEnv("review-evidence"))
			if err == nil || out.Len() != 0 {
				t.Errorf("ambiguous evidence accepted: err=%v applied=%s", err, out.String())
			}
		})
	}
	t.Run("true_error_exit_zero_refused", func(t *testing.T) {
		c, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, true, 0))
		if err := c.handle(noticeEnv("review-trap")); err == nil || out.Len() != 0 {
			t.Fatal("is_error:true accepted")
		}
	})
	t.Run("nonzero_exit_clean_result_refused", func(t *testing.T) {
		c, out, _, _ := fakeChild(t, 7, good)
		if err := c.handle(noticeEnv("review-exit")); err == nil || out.Len() != 0 {
			t.Fatal("nonzero exit accepted")
		}
	})
	t.Run("trailing_object_refused", func(t *testing.T) {
		c, out, _, _ := fakeChild(t, 0, good+"{}\n")
		if err := c.handle(noticeEnv("review-trailing")); err == nil || out.Len() != 0 {
			t.Fatal("two objects accepted")
		}
	})
}

func TestReviewInvalidIDCannotEscapeOutput(t *testing.T) {
	c, out, ws, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	victim := filepath.Join(filepath.Dir(ws), "review-victim.receipt.json")
	sentinel := []byte("synthetic sentinel, no credential data\n")
	if err := os.WriteFile(victim, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	env := noticeEnv("../../review-victim")
	err := c.handle(env)
	got, readErr := os.ReadFile(victim)
	if err == nil || err.Error() != "invalid_notice_id" || out.Len() != 0 || readErr != nil || !bytes.Equal(got, sentinel) {
		t.Errorf("rejected ID wrote outside workspace: err=%v read=%v content=%s", err, readErr, got)
	}
}

func TestReviewReceiptSymlinkCannotOverwrite(t *testing.T) {
	c, out, ws, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	os.MkdirAll(c.cfg.outDir, 0700)
	victim := filepath.Join(ws, "synthetic-sentinel")
	sentinel := []byte("keep synthetic file intact\n")
	if err := os.WriteFile(victim, sentinel, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(c.cfg.outDir, "review-link.receipt.json")); err != nil {
		t.Fatal(err)
	}
	err := c.handle(noticeEnv("review-link"))
	got, _ := os.ReadFile(victim)
	if err == nil || out.Len() != 0 || !bytes.Equal(got, sentinel) {
		t.Errorf("receipt followed symlink and claimed applied: err=%v content=%s applied=%s", err, got, out)
	}
}

func TestReviewDuplicatePreservesOriginalReceipt(t *testing.T) {
	c, _, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	env := noticeEnv("review-duplicate")
	if err := c.handle(env); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(c.cfg.outDir, env.Notice.ID+".receipt.json")
	before, _ := os.ReadFile(p)
	if err := c.handle(env); err == nil {
		t.Fatal("duplicate accepted")
	}
	after, _ := os.ReadFile(p)
	if !bytes.Equal(before, after) {
		t.Errorf("duplicate destroys successful receipt: before=%s after=%s", before, after)
	}
}

func TestReviewStoppedBeforeReceiptNeverApplied(t *testing.T) {
	for _, cancelDuringWrite := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelDuringWrite), func(t *testing.T) {
			c, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.parent = ctx
			entered, release := make(chan struct{}), make(chan struct{})
			c.syncReceipt = func(f *os.File) error { close(entered); <-release; return f.Sync() }
			done := make(chan error, 1)
			go func() { done <- c.handle(noticeEnv("receipt-stop")) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("receipt durability barrier not reached")
			}
			if cancelDuringWrite {
				cancel()
			}
			close(release)
			err := <-done
			if cancelDuringWrite {
				if err == nil || out.Len() != 0 {
					t.Fatalf("applied after cancellation: %v %s", err, out)
				}
			} else if err != nil || out.Len() == 0 {
				t.Fatalf("successful control refused: %v", err)
			}
		})
	}
}

func TestReceiptFIFORefusedWithoutBlocking(t *testing.T) {
	c, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	if err := os.MkdirAll(c.cfg.outDir, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(c.cfg.outDir, "fifo.receipt.json")
	if err := syscall.Mkfifo(p, 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.handle(noticeEnv("fifo")); err == nil || out.Len() != 0 {
		t.Fatalf("FIFO accepted: %v", err)
	}
}

func TestReviewStdoutBoundAndReceiptFailureControls(t *testing.T) {
	t.Run("capture_bound", func(t *testing.T) {
		c, out, _, _ := fakeChild(t, 0, strings.Repeat("x", maxCaptureBytes+1))
		err := c.handle(noticeEnv("review-bound"))
		if err == nil || err.Error() != "result_truncated" || out.Len() != 0 {
			t.Fatalf("bound failed: %v", err)
		}
	})
	t.Run("receipt_failure", func(t *testing.T) {
		c, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
		os.MkdirAll(filepath.Join(c.cfg.outDir, "review-fail.receipt.json"), 0700)
		err := c.handle(noticeEnv("review-fail"))
		if err == nil || err.Error() != "receipt_write_failed" || out.Len() != 0 {
			t.Fatalf("receipt failure applied: %v %s", err, out)
		}
	})
}

func TestReviewResultTextAbsentFromReceipt(t *testing.T) {
	marker := "SYNTHETIC_PAYLOAD_MARKER_123"
	payload := strings.Replace(resultJSON(fixedSession, false, 0), `"result":"done"`, fmt.Sprintf(`"result":%q`, marker), 1)
	c, _, _, _ := fakeChild(t, 0, payload)
	if err := c.handle(noticeEnv("review-payload")); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(c.cfg.outDir, "review-payload.receipt.json"))
	if bytes.Contains(raw, []byte(marker)) {
		t.Error("native result text copied into receipt")
	}
}

func TestReviewRunClaudeNoSameGroupOrphan(t *testing.T) {
	ws := canonicalTempDir(t)
	childMarker := filepath.Join(ws, "owned-child-pid")
	parentMarker := filepath.Join(ws, "owned-parent-pid")
	script := `trap 'exit 0' TERM
/bin/sh -c 'trap "" TERM; echo "$$" > "$1"; exec /bin/sleep 30' sh "$1" >/dev/null 2>&1 &
echo "$$" > "$2"
while :; do /bin/sleep 1 & wait $!; done
`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runClaude("/bin/sh", ws)(ctx, []string{"-c", script, "sh", childMarker, parentMarker}, strings.NewReader(""), io.Discard, io.Discard)
		done <- err
	}()
	var childPID, parentPID int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(childMarker)
		p, _ := os.ReadFile(parentMarker)
		childPID, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		parentPID, _ = strconv.Atoi(strings.TrimSpace(string(p)))
		if childPID > 0 && parentPID > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID <= 0 || parentPID <= 0 {
		t.Fatal("owned fixture did not publish PIDs")
	}
	group, err := syscall.Getpgid(childPID)
	if err != nil || group != parentPID {
		t.Fatalf("ownership/group not established: %d %d %v", group, parentPID, err)
	}
	// Only kill the exact PID from our fixture, after verifying its group.
	defer func() {
		if g, e := syscall.Getpgid(childPID); e == nil && g == parentPID {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("runClaude did not return after cancellation")
	}
	if err := syscall.Kill(childPID, 0); err == nil {
		t.Errorf("owned same-group descendant still alive after runClaude returned: child=%d group=%d", childPID, parentPID)
	}
}
