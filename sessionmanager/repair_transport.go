//go:build unix

package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const endpointFileName = "endpoint.json"
const tokenFileName = "token"

func waitForProcessExit(ctx context.Context, pid int, timeout time.Duration) (bool, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(providerProcessPollPeriod)
	defer ticker.Stop()
	for {
		alive, err := processAlive(pid)
		if err != nil {
			return false, err
		}
		if !alive {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			return false, nil
		case <-ticker.C:
		}
	}
}

func codexProcessMatches(
	ctx context.Context,
	pid int,
	executable string,
	identity string,
) (bool, error) {
	if pid <= 0 || strings.TrimSpace(identity) == "" {
		return false, nil
	}
	commandLine, err := processCommandLine(ctx, pid)
	if err != nil {
		return false, err
	}
	if !bytes.Contains(commandLine, []byte(identity)) {
		return false, nil
	}
	if base := filepath.Base(strings.TrimSpace(executable)); base != "" &&
		!bytes.Contains(commandLine, []byte(base)) {
		return false, nil
	}
	return true, nil
}

func processCommandLine(ctx context.Context, pid int) ([]byte, error) {
	if data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")); err == nil {
		return data, nil
	}
	// Ask BSD and procps ps implementations for an untruncated command line:
	// the durable listen URL can appear after a long executable path or host argv.
	command := exec.CommandContext(ctx, "ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid))
	output, err := command.Output()
	if err != nil {
		alive, aliveErr := processAlive(pid)
		if aliveErr != nil {
			return nil, aliveErr
		}
		if !alive {
			return nil, nil
		}
		return nil, fmt.Errorf("inspecting provider pid %d command line: %w", pid, err)
	}
	return output, nil
}

func sweepStaleTransports(ctx context.Context, layout Layout) (RepairReport, error) {
	var report RepairReport
	if err := sweepEndpointMetadata(ctx, layout, &report); err != nil {
		return report, err
	}
	visited := make(map[string]struct{})
	for _, root := range []string{
		layout.SocketPath,
		layout.AttachmentsDir,
	} {
		if err := sweepSocketTree(ctx, root, &report, visited); err != nil {
			return report, err
		}
	}
	socketRoot := filepath.Dir(layout.SocketPath)
	if socketRoot != layout.StateDir {
		if err := sweepSocketTree(ctx, socketRoot, &report, visited); err != nil {
			return report, err
		}
	}
	if err := sweepStateTransportRoots(ctx, filepath.Dir(layout.StateDir), &report, visited); err != nil {
		return report, err
	}
	// Attachment directories are swept last, after the socket sweeps above have
	// removed the dead sockets that would otherwise keep a finished board's
	// directory looking occupied.
	parents, err := attachmentDirParents(filepath.Dir(layout.StateDir), layout)
	if err != nil {
		return report, err
	}
	if err := sweepStaleAttachmentDirs(ctx, parents, &report); err != nil {
		return report, err
	}
	return report, nil
}

// attachmentDirParents lists every directory that can contain an
// attachments-<hex> child: the shared socket root (where the Darwin short-path
// fallback puts them, and where the leak was observed), the parent of this
// layout's own attachments directory, and every per-board state directory.
func attachmentDirParents(managerRoot string, layout Layout) ([]string, error) {
	parents := []string{
		filepath.Dir(layout.SocketPath),
		filepath.Dir(layout.AttachmentsDir),
	}
	entries, err := os.ReadDir(managerRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return parents, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading session manager state root for attachment GC: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			parents = append(parents, filepath.Join(managerRoot, entry.Name()))
		}
	}
	return parents, nil
}

// sweepStaleAttachmentDirs removes attachments-<hex> directories that belong to
// no live attachment. "Belongs to no live attachment" is read off the directory
// itself: an attachment is a proxy socket inside it, so a directory with no
// entry at all has none, and one that still holds anything is left untouched.
//
// The removal is os.Remove, never os.RemoveAll: a directory holding a live
// proxy socket must survive this sweep even if the emptiness check above were
// wrong.
func sweepStaleAttachmentDirs(
	ctx context.Context,
	parents []string,
	report *RepairReport,
) error {
	swept := make(map[string]struct{})
	for _, parent := range parents {
		children, err := os.ReadDir(parent)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("reading attachment directory root %s: %w", parent, err)
		}
		for _, child := range children {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !child.IsDir() || !strings.HasPrefix(child.Name(), "attachments-") {
				continue
			}
			path := filepath.Clean(filepath.Join(parent, child.Name()))
			if _, seen := swept[path]; seen {
				continue
			}
			swept[path] = struct{}{}
			entries, err := os.ReadDir(path)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			// A failed or partial read is not an absence of attachments.
			if err != nil {
				return fmt.Errorf("reading attachment directory %s: %w", path, err)
			}
			if len(entries) > 0 {
				if attachmentDirHoldsLiveSocket(path, entries) {
					report.LiveAttachmentDirsPreserved++
				}
				continue
			}
			if err := os.Remove(path); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				// Lost a race with a concurrent attach: something appeared
				// between the read and the removal. Leave it to the next sweep.
				if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
					continue
				}
				return fmt.Errorf("removing stale attachment directory %s: %w", path, err)
			}
			report.StaleAttachmentDirsRemoved++
		}
	}
	return nil
}

func attachmentDirHoldsLiveSocket(dir string, entries []fs.DirEntry) bool {
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			continue
		}
		if unixSocketListening(filepath.Join(dir, entry.Name())) {
			return true
		}
	}
	return false
}

func sweepStateTransportRoots(
	ctx context.Context,
	managerRoot string,
	report *RepairReport,
	visited map[string]struct{},
) error {
	entries, err := os.ReadDir(managerRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		stateDir := filepath.Join(managerRoot, entry.Name())
		if err := sweepSocketTree(
			ctx,
			filepath.Join(stateDir, "control.sock"),
			report,
			visited,
		); err != nil {
			return err
		}
		children, err := os.ReadDir(stateDir)
		if err != nil {
			return err
		}
		for _, child := range children {
			if child.IsDir() && strings.HasPrefix(child.Name(), "attachments-") {
				if err := sweepSocketTree(
					ctx,
					filepath.Join(stateDir, child.Name()),
					report,
					visited,
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func sweepEndpointMetadata(
	ctx context.Context,
	layout Layout,
	report *RepairReport,
) error {
	managerRoot := filepath.Dir(layout.StateDir)
	entries, err := os.ReadDir(managerRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading session manager state root for endpoint GC: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}
		stateDir := filepath.Join(managerRoot, entry.Name())
		endpointPath := filepath.Join(stateDir, endpointFileName)
		data, err := os.ReadFile(endpointPath)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("reading endpoint metadata %s: %w", endpointPath, err)
		}
		var endpoint struct {
			BoardFingerprint string `json:"board_fingerprint"`
			SocketPath       string `json:"socket_path"`
		}
		decodeErr := json.Unmarshal(data, &endpoint)
		if decodeErr == nil &&
			endpoint.BoardFingerprint == entry.Name() &&
			filepath.IsAbs(endpoint.SocketPath) {
			probeErr := probeUnixSocket(endpoint.SocketPath, transportProbeTimeout)
			if probeErr == nil {
				report.LiveEndpointsPreserved++
				continue
			}
			if !unixSocketKnownAbsent(probeErr) {
				// An unknown transport/permission result proves no absence.
				continue
			}
		}
		if err := os.Remove(endpointPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing stale endpoint metadata %s: %w", endpointPath, err)
		}
		tokenPath := filepath.Join(stateDir, tokenFileName)
		if err := os.Remove(tokenPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing stale manager token %s: %w", tokenPath, err)
		}
		report.StaleEndpointsRemoved++
	}
	return nil
}

func sweepSocketTree(
	ctx context.Context,
	root string,
	report *RepairReport,
	visited map[string]struct{},
) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSocket == 0 {
			return nil
		}
		path = filepath.Clean(path)
		if _, ok := visited[path]; ok {
			return nil
		}
		visited[path] = struct{}{}
		probeErr := probeUnixSocket(path, transportProbeTimeout)
		if probeErr == nil {
			report.LiveSocketsPreserved++
			return nil
		}
		if !unixSocketKnownAbsent(probeErr) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		removed, err := removeSameOwnedSocket(path, info)
		if err != nil {
			return fmt.Errorf("removing stale Unix socket %s: %w", path, err)
		}
		if removed {
			report.StaleSocketsRemoved++
		}
		return nil
	})
}

func unixSocketListening(path string) bool {
	return probeUnixSocket(path, transportProbeTimeout) == nil
}

func probeUnixSocket(path string, timeout time.Duration) error {
	connection, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return err
	}
	_ = connection.Close()
	return nil
}

func unixSocketKnownAbsent(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist)
}

// Revalidate the observed socket immediately before path-based removal. This
// preserves a changed occupant; it is not an atomic unlink against same-user
// renames between Lstat and Remove. Runtime directories must remain private.
func removeSameOwnedSocket(path string, observed os.FileInfo) (bool, error) {
	if observed == nil || observed.Mode()&os.ModeSocket == 0 {
		return false, nil
	}
	owner, ok := observed.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return false, nil
	}
	current, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	currentOwner, ok := current.Sys().(*syscall.Stat_t)
	if !ok || currentOwner.Uid != uint32(os.Getuid()) || current.Mode()&os.ModeSocket == 0 || !os.SameFile(observed, current) {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
