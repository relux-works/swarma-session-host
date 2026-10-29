//go:build unix

package sessionmanager

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func legacyPhysicalSocketCode(t *testing.T, err error, code string) {
	t.Helper()
	var refusal *PublicError
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("socket refusal = %v, want %s", err, code)
	}
}

func legacyPhysicalSocketUnpublished(t *testing.T, layout Layout) {
	t.Helper()
	for _, path := range []string{layout.EndpointFile, layout.TokenFile} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("refused control setup published %s: %v", filepath.Base(path), err)
		}
	}
}

// Hashes and modes measure effects on this test's own admitted host storage;
// timestamps, kernel lock ownership, and foreign files are not sampled.
func legacyPhysicalSocketImage(t *testing.T, root string) map[string]string {
	t.Helper()
	image := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		value := fmt.Sprint(info.Mode())
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value += fmt.Sprintf(":%x", sha256.Sum256(data))
		}
		image[rel] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return image
}

// Each attempt executes the real server and status converter, then joins it.
// Unexpected readiness returns normally so the intended refusal/no-effect
// assertion fails; a setup timeout or failed join is never a control kill.
func legacyPhysicalSocketAttempt(t *testing.T, host *SessionHost, onReady func()) (err error, ready bool, factories, requests int64) {
	t.Helper()
	var factoryCalls, requestCalls atomic.Int64
	server, err := NewControlServer(host, func(s *ControlServer) http.Handler {
		factoryCalls.Add(1)
		handler, e := NewProtocol8Handler(host, s.Status, nil)
		if e != nil {
			panic(e)
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCalls.Add(1)
			handler.ServeHTTP(w, r)
		})
	}, WithControlServerVersion("socket-contract-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			select {
			case e := <-done:
				if e != nil {
					t.Errorf("control fixture cleanup: %v", e)
				}
			case <-time.After(6 * time.Second):
				t.Error("control fixture failed to join; qualification unavailable")
			}
		}
	}()
	select {
	case err = <-done:
		joined = true
	case <-server.Ready():
		ready = true
		if onReady != nil {
			onReady()
		}
		cancel()
		select {
		case err = <-done:
			joined = true
		case <-time.After(6 * time.Second):
			t.Fatal("control fixture failed to join; qualification unavailable")
		}
	case <-ctx.Done():
		t.Fatal("control fixture timed out before refusal/readiness; qualification unavailable")
	}
	return err, ready, factoryCalls.Load(), requestCalls.Load()
}

// Missing and existing private directories are valid neighbors of the directory
// refusals. This proves actual pre-bind admission and creation, not kernel bind
// failure, a standalone selector's use, or native Darwin execution.
func TestControlSocketAcceptsPrivateAndMissingDirectory(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint("missing-", missing), func(t *testing.T) {
			layout := legacyPhysicalLayout(t, "socket-positive")
			if missing {
				layout.SocketPath = filepath.Join(filepath.Dir(layout.SocketPath), "new", "c.sock")
			}
			host, err := legacyPhysicalNewHost(t, layout)
			if err != nil {
				t.Fatal(err)
			}
			err, ready, factories, requests := legacyPhysicalSocketAttempt(t, host, func() {
				info, e := os.Lstat(filepath.Dir(layout.SocketPath))
				if e != nil || info.Mode() != os.ModeDir|0700 {
					t.Fatalf("private socket directory: %v %v", info, e)
				}
				owner, ok := info.Sys().(*syscall.Stat_t)
				if !ok || owner.Uid != uint32(os.Getuid()) {
					t.Fatal("socket directory has wrong actual uid")
				}
				for _, path := range []string{layout.SocketPath, layout.EndpointFile, layout.TokenFile} {
					info, e := os.Lstat(path)
					if e != nil || info.Mode().Perm() != 0600 {
						t.Fatalf("private publication %s: %v %v", filepath.Base(path), info, e)
					}
				}
			})
			if err != nil || !ready || factories != 1 || requests < 1 {
				t.Fatalf("private positive: %v ready=%v factory=%d requests=%d", err, ready, factories, requests)
			}
		})
	}
}

// An unsafe existing directory must fail before generic chmod, token/endpoint
// publication, logger creation or converter invocation. All plants are owned.
func TestControlSocketRejectsUnsafeDirectoryBeforeEffects(t *testing.T) {
	for _, kind := range []string{"loose", "symlink", "file"} {
		t.Run(kind, func(t *testing.T) {
			layout := legacyPhysicalLayout(t, "socket-directory-refusal")
			root := filepath.Dir(layout.SocketPath)
			path := filepath.Join(root, "plant")
			target := ""
			switch kind {
			case "loose":
				if err := os.Mkdir(path, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target = t.TempDir()
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			var targetBefore os.FileInfo
			if target != "" {
				targetBefore, err = os.Lstat(target)
				if err != nil {
					t.Fatal(err)
				}
			}
			layout.SocketPath = filepath.Join(path, "c.sock")
			host, err := legacyPhysicalNewHost(t, layout)
			if err != nil {
				t.Fatal(err)
			}
			stateBefore := legacyPhysicalSocketImage(t, layout.StateDir)
			err, ready, factories, requests := legacyPhysicalSocketAttempt(t, host, nil)
			legacyPhysicalSocketCode(t, err, ErrorBoardlessSocketUnavailable)
			if !strings.Contains(err.Error(), "directory must be owned") || ready || factories != 0 || requests != 0 {
				t.Fatalf("wrong pre-effect directory refusal: %v %v %d %d", err, ready, factories, requests)
			}
			legacyPhysicalSocketUnpublished(t, layout)
			if !reflect.DeepEqual(stateBefore, legacyPhysicalSocketImage(t, layout.StateDir)) {
				t.Fatal("directory refusal wrote host state")
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("refusal changed planted directory entry: %v", err)
			}
			if target != "" {
				after, e := os.Lstat(target)
				if e != nil || !os.SameFile(targetBefore, after) || targetBefore.Mode() != after.Mode() {
					t.Fatal("refusal followed/chmodded symlink target")
				}
			}
		})
	}
}

// Relative paths fail in the actual constructor before any server effects; the
// original pure helper independently classifies the same nonabsolute input.
func TestControlSocketRejectsRelativePathBeforeEffects(t *testing.T) {
	legacyPhysicalSocketCode(t, checkBoardlessSocketLength("relative/control.sock"), ErrorBoardlessSocketUnavailable)
	layout := legacyPhysicalLayout(t, "socket-relative")
	host, err := legacyPhysicalNewHost(t, layout)
	if err != nil {
		t.Fatal(err)
	}
	before := legacyPhysicalSocketImage(t, layout.StateDir)
	host.options.Layout.SocketPath = "relative/control.sock"
	calls := 0
	server, err := NewControlServer(host, func(*ControlServer) http.Handler { calls++; return http.NotFoundHandler() })
	if err == nil || server != nil || !strings.Contains(err.Error(), "absolute control runtime paths") || calls != 0 {
		t.Fatalf("relative constructor: %v %v calls=%d", server, err, calls)
	}
	legacyPhysicalSocketUnpublished(t, layout)
	if !reflect.DeepEqual(before, legacyPhysicalSocketImage(t, layout.StateDir)) {
		t.Fatal("relative constructor wrote state")
	}
}

// A genuinely foreign UID is sampled read-only through the pure checker. No
// ensure/Mkdir/Chmod/Serve call ever targets it. This does not qualify runtime
// mutation refusal on foreign paths; unsuitable hosts explicitly leave it unrun.
func TestControlSocketDirectoryRejectsForeignUIDReadOnly(t *testing.T) {
	for _, path := range []string{"/root", "/var/root"} {
		before, err := os.Lstat(path)
		if err != nil || before.Mode() != os.ModeDir|0700 {
			continue
		}
		owner, ok := before.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid == uint32(os.Getuid()) {
			continue
		}
		err = checkBoardlessSocketDirectory(path)
		legacyPhysicalSocketCode(t, err, ErrorBoardlessSocketUnavailable)
		if !strings.Contains(err.Error(), "directory must be owned") {
			t.Fatalf("wrong uid refusal: %v", err)
		}
		after, err := os.Lstat(path)
		if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
			t.Fatal("read-only UID check changed metadata")
		}
		t.Logf("pure helper rejected actual foreign uid=%d with private mode; no mutation or server attempted", owner.Uid)
		return
	}
	t.Skip("no read-only foreign-owned mode0700 directory available; UID branch unqualified on this host")
}

// A live owned listener and foreign entry shapes remain unchanged when startup
// refuses. The source's liveness/error classification is asserted explicitly;
// no timeout, bind error or occupied publication is accepted as that refusal.
func TestControlSocketPreservesOccupiedAndForeignEntries(t *testing.T) {
	for _, kind := range []string{"listener", "symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			layout := legacyPhysicalLayout(t, "socket-occupied")
			host, err := legacyPhysicalNewHost(t, layout)
			if err != nil {
				t.Fatal(err)
			}
			want := "existing path is not an owned socket"
			switch kind {
			case "listener":
				listener, e := net.Listen("unix", layout.SocketPath)
				if e != nil {
					t.Fatal(e)
				}
				listener.(*net.UnixListener).SetUnlinkOnClose(false)
				t.Cleanup(func() { _ = listener.Close() })
				want = "existing listener is reachable"
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				if e := os.WriteFile(target, []byte("preserve"), 0600); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(target, layout.SocketPath); e != nil {
					t.Fatal(e)
				}
			case "directory":
				if e := os.Mkdir(layout.SocketPath, 0700); e != nil {
					t.Fatal(e)
				}
			}
			before, err := os.Lstat(layout.SocketPath)
			if err != nil {
				t.Fatal(err)
			}
			err, ready, factories, requests := legacyPhysicalSocketAttempt(t, host, nil)
			legacyPhysicalSocketCode(t, err, ErrorBoardlessSocketUnavailable)
			if !strings.Contains(err.Error(), want) || ready || factories != 0 || requests != 0 {
				t.Fatalf("occupied refusal: %v %v %d %d", err, ready, factories, requests)
			}
			legacyPhysicalSocketUnpublished(t, layout)
			after, err := os.Lstat(layout.SocketPath)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("occupied entry was deleted/replaced: %v", err)
			}
			if kind == "listener" {
				connection, e := net.DialTimeout("unix", layout.SocketPath, time.Second)
				if e != nil {
					t.Fatalf("occupied listener no longer reachable: %v", e)
				}
				_ = connection.Close()
			}
		})
	}
}

// EPROTOTYPE from a real owned datagram socket is unknown stream liveness, not
// absence. The repair-before-bind path must preserve it and refuse publication.
func TestControlSocketPreservesUnknownTransport(t *testing.T) {
	layout := legacyPhysicalLayout(t, "socket-unknown")
	host, err := legacyPhysicalNewHost(t, layout)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Net: "unixgram", Name: layout.SocketPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	before, err := os.Lstat(layout.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := net.DialTimeout("unix", layout.SocketPath, time.Second)
	if err == nil {
		_ = probe.Close()
		t.Fatal("unknown transport plant unexpectedly accepts stream connections")
	}
	if !errors.Is(err, syscall.EPROTOTYPE) {
		t.Fatalf("unknown transport plant did not reach EPROTOTYPE: %v", err)
	}
	err, ready, factories, requests := legacyPhysicalSocketAttempt(t, host, nil)
	var refusal *PublicError
	if !errors.As(err, &refusal) || refusal.Code != ErrorBoardlessSocketUnavailable || !strings.Contains(refusal.Message, "liveness is unknown") {
		t.Errorf("unknown socket was not refused as unknown liveness: %v", err)
	}
	if ready || factories != 0 || requests != 0 {
		t.Errorf("unknown socket reached publication/converter: ready=%v factory=%d requests=%d", ready, factories, requests)
	}
	after, statErr := os.Lstat(layout.SocketPath)
	if statErr != nil || !os.SameFile(before, after) {
		t.Errorf("unknown socket was deleted or replaced: %v", statErr)
	}
	legacyPhysicalSocketUnpublished(t, layout)
}

// Shutdown owns its original listener, not a replacement installed at that path.
// Both sockets are isolated fixture-owned; no foreign host path is modified.
func TestControlSocketShutdownPreservesReplacement(t *testing.T) {
	layout := legacyPhysicalLayout(t, "socket-replaced")
	host, err := legacyPhysicalNewHost(t, layout)
	if err != nil {
		t.Fatal(err)
	}
	var replacement net.Listener
	var before os.FileInfo
	err, ready, factories, requests := legacyPhysicalSocketAttempt(t, host, func() {
		original, e := os.Lstat(layout.SocketPath)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.Remove(layout.SocketPath); e != nil {
			t.Fatal(e)
		}
		replacement, e = net.Listen("unix", layout.SocketPath)
		if e != nil {
			t.Fatal(e)
		}
		replacement.(*net.UnixListener).SetUnlinkOnClose(false)
		t.Cleanup(func() { _ = replacement.Close() })
		before, e = os.Lstat(layout.SocketPath)
		if e != nil || os.SameFile(original, before) {
			t.Fatalf("replacement socket plant did not apply: %v", e)
		}
	})
	if err != nil || !ready || factories != 1 || requests < 1 || replacement == nil {
		t.Fatalf("replacement setup: %v ready=%v factory=%d requests=%d", err, ready, factories, requests)
	}
	after, err := os.Lstat(layout.SocketPath)
	if err != nil || !os.SameFile(before, after) {
		t.Errorf("shutdown deleted/replaced a socket outside its listener custody: %v", err)
	}
	connection, err := net.DialTimeout("unix", layout.SocketPath, time.Second)
	if err != nil {
		t.Errorf("replacement listener lost its path on server shutdown: %v", err)
	} else {
		_ = connection.Close()
	}
}

// Private fallback paths are reusable by their owner; planted loose directories
// and symlinks refuse without being chmodded or replaced. Overlong paths refuse
// before filesystem effects. These are real entries under isolated fixtures.
func TestBoardlessSocket(t *testing.T) {
	for _, kind := range []string{"private", "loose", "symlink", "file", "no-path-fits"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			fingerprint := kind[:1]
			dir := filepath.Join(root, fmt.Sprintf("tbbl-%d-%s", os.Getuid(), fingerprint))
			switch kind {
			case "loose":
				if err := os.Mkdir(dir, 0755); err != nil {
					t.Fatal(err)
				}
				if info, err := os.Lstat(dir); err != nil || info.Mode().Perm() == 0700 {
					t.Fatalf("loose-mode plant did not apply: %v %v", info, err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), dir); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.WriteFile(dir, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			case "no-path-fits":
				root = filepath.Join(root, strings.Repeat("x", 120))
			}
			before, beforeErr := os.Lstat(dir)
			// Directory ownership tests are independent of sun_path capacity, so
			// their t.TempDir can be arbitrarily long. The production selector
			// composes this exact exclusive-create/check function with length checks.
			path := ""
			var err error
			if kind == "no-path-fits" {
				path, err = boardlessSocketPathAt(root, strings.Repeat("a", 64))
			} else {
				err = ensureBoardlessSocketDirectory(dir)
			}
			if kind == "private" {
				if err != nil {
					t.Fatal(err)
				}
				if err := ensureBoardlessSocketDirectory(dir); err != nil {
					t.Fatalf("private reuse: %v", err)
				}
				if err := checkBoardlessSocketDirectory(dir); err != nil {
					t.Fatal(err)
				}
			} else {
				legacyPhysicalSocketCode(t, err, ErrorBoardlessSocketUnavailable)
				var refusal *PublicError
				if !errors.As(err, &refusal) {
					t.Fatal(err)
				}
				want := "directory must be owned"
				if kind == "no-path-fits" {
					want = "path exceeds"
				}
				if !strings.Contains(refusal.Message, want) {
					t.Fatalf("wrong refusal, plant was not exercised: %v", err)
				}
				if path != "" {
					t.Fatalf("refusal returned a path: %q", path)
				}
				after, afterErr := os.Lstat(dir)
				if beforeErr == nil && (afterErr != nil || after.Mode() != before.Mode() || !os.SameFile(before, after)) {
					t.Fatal("refusal changed planted metadata")
				}
				if kind == "no-path-fits" {
					if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("overlong root was created: %v", err)
					}
				}
			}
		})
	}
	// Platform capacities include the terminating NUL; equality must refuse.
	t.Run("platform-boundary", func(t *testing.T) {
		if boardlessSocketLimit("darwin") != 104 || boardlessSocketLimit("linux") != 108 {
			t.Fatal("wrong platform limits")
		}
		limit := boardlessSocketLimit(runtime.GOOS)
		if err := checkBoardlessSocketLength("/" + strings.Repeat("s", limit-2)); err != nil {
			t.Fatal(err)
		}
		legacyPhysicalSocketCode(t, checkBoardlessSocketLength("/"+strings.Repeat("s", limit-1)), ErrorBoardlessSocketUnavailable)
	})
	// A caller-supplied layout cannot bypass the final bind guard, and rejection
	// precedes state setup or generic permission changes.
	t.Run("pre-bind", func(t *testing.T) {
		layout := legacyPhysicalLayout(t, "socket-pre-bind")
		layout.SocketPath = filepath.Join(t.TempDir(), strings.Repeat("s", 120))
		manager, err := legacyPhysicalNewHost(t, layout)
		if err != nil {
			t.Fatal(err)
		}
		// NewHost now admits its storage at construction. The original Serve
		// no-effect claim is measured against that actual admitted image.
		before := legacyPhysicalSocketImage(t, layout.StateDir)
		err, ready, factories, requests := legacyPhysicalSocketAttempt(t, manager, nil)
		legacyPhysicalSocketCode(t, err, ErrorBoardlessSocketUnavailable)
		if ready || factories != 0 || requests != 0 {
			t.Fatal("overlong layout reached the converter or became ready")
		}
		legacyPhysicalSocketUnpublished(t, layout)
		if after := legacyPhysicalSocketImage(t, layout.StateDir); !reflect.DeepEqual(before, after) {
			t.Fatal("overlong layout wrote state during Serve")
		}
	})
}
