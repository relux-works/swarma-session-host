package sessionclient

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var ErrStartupObservation = errors.New("session manager startup observation is unavailable")
var ErrStartupEndpointChanged = errors.New("session manager endpoint identity changed")
var ErrStartupPrivatePath = errors.New("session manager startup requires private canonical paths")

func startupFatal(err error) bool {
	return errors.Is(err, ErrStartupObservation) || errors.Is(err, ErrStartupPrivatePath) ||
		errors.Is(err, ErrStartupEndpointChanged) || errors.Is(err, ErrNamespaceMismatch) ||
		errors.Is(err, context.Canceled)
}

func validateStartupLayout(l StartupLayout) error {
	if strings.TrimSpace(l.Namespace) == "" {
		return ErrStartupPrivatePath
	}
	paths := []string{l.EndpointFile, l.TokenFile, l.LockFile, l.SocketPath}
	seen := map[string]bool{}
	for _, p := range paths {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || seen[p] {
			return ErrStartupPrivatePath
		}
		seen[p] = true
		dir := filepath.Dir(p)
		if info, err := os.Lstat(dir); err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
				return ErrStartupPrivatePath
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return ErrStartupPrivatePath
		}
	}
	if filepath.Dir(l.EndpointFile) != filepath.Dir(l.TokenFile) || filepath.Dir(l.TokenFile) != filepath.Dir(l.LockFile) {
		return ErrStartupPrivatePath
	}
	for _, p := range []string{l.EndpointFile, l.TokenFile, l.LockFile} {
		if info, err := os.Lstat(p); err == nil {
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
				return ErrStartupPrivatePath
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return ErrStartupPrivatePath
		}
	}
	return nil
}
func startupEndpoint(l StartupLayout) (Endpoint, error) {
	if err := validateStartupLayout(l); err != nil {
		return Endpoint{}, err
	}
	ep, err := ReadEndpoint(l.EndpointFiles)
	if errors.Is(err, ErrManagerNotRunning) {
		return Endpoint{}, ErrManagerNotRunning
	}
	if errors.Is(err, ErrNamespaceMismatch) {
		return Endpoint{}, ErrNamespaceMismatch
	}
	if err != nil {
		return Endpoint{}, ErrStartupObservation
	}
	if ep.SocketPath != l.SocketPath || ep.PID <= 1 || strings.TrimSpace(ep.InstanceID) == "" {
		return Endpoint{}, ErrStartupEndpointChanged
	}
	return ep, nil
}
func (r *startupRuntime) dial(l StartupLayout) (*Client, error) {
	ep, err := startupEndpoint(l)
	if err != nil {
		return nil, err
	}
	if info, err := os.Lstat(ep.SocketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
			return nil, ErrStartupPrivatePath
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, ErrStartupPrivatePath
	}
	c, err := Dial(l.EndpointFiles)
	if err != nil {
		if errors.Is(err, ErrManagerNotRunning) {
			return nil, err
		}
		return nil, ErrStartupObservation
	}
	return c, nil
}

// startupSourceError hides private filesystem/decoder diagnostics while keeping
// the actual cause available to errors.Is/As for a trusted caller's handling.
type startupSourceError struct{ cause error }

func (e *startupSourceError) Error() string { return "session manager startup source operation failed" }
func (e *startupSourceError) Unwrap() error { return e.cause }

func (r *startupRuntime) status(ctx context.Context, c *Client, l StartupLayout) (StartupStatus, error) {
	var s StartupStatus
	response, err := c.RoundTrip(ctx, "GET", "/v1/status", nil, true)
	if err != nil {
		return s, &startupSourceError{cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return s, ErrStartupObservation
	}
	if err = json.NewDecoder(response.Body).Decode(&s); err != nil {
		return s, ErrStartupObservation
	}
	ep, err := startupEndpoint(l)
	if err != nil {
		return s, err
	}
	token, err := ReadPrivateFile(l.TokenFile)
	if err != nil {
		return s, ErrStartupObservation
	}
	if c.Token != strings.TrimSpace(string(token)) || c.SocketPath != ep.SocketPath || s.PID != ep.PID || s.InstanceID != ep.InstanceID || s.Namespace != ep.Namespace {
		return s, ErrStartupEndpointChanged
	}
	return s, nil
}
func (r *startupRuntime) lockHeld(ctx context.Context, l StartupLayout) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if r.options.Source == nil {
		return false, ErrStartupAuthorityUnavailable
	}
	held, err := r.options.Source.LockHeld(ctx, l)
	if err != nil {
		return false, ErrStartupObservation
	}
	return held, nil
}
func (r *startupRuntime) processAlive(ctx context.Context, ep Endpoint) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if r.options.Source == nil {
		return false, ErrStartupAuthorityUnavailable
	}
	state, err := r.options.Source.Process(ctx, ep)
	if err != nil {
		return false, ErrStartupObservation
	}
	switch state {
	case StartupProcessPresent:
		return true, nil
	case StartupProcessAbsent:
		return false, nil
	}
	return false, ErrStartupObservation
}
func (r *startupRuntime) validateReplacement(ctx context.Context, l StartupLayout, recorded Endpoint, cap StartupReplacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cap == nil || !sameEndpointInstance(cap.Endpoint(), recorded) {
		return ErrStartupAuthorityUnavailable
	}
	current, err := startupEndpoint(l)
	if err != nil {
		return err
	}
	if !sameEndpointInstance(current, recorded) {
		return ErrStartupEndpointChanged
	}
	held, err := r.lockHeld(ctx, l)
	if err != nil {
		return err
	}
	if !held {
		return ErrStartupEndpointChanged
	}
	alive, err := r.processAlive(ctx, recorded)
	if err != nil {
		return err
	}
	if !alive {
		return ErrStartupEndpointChanged
	}
	return r.guardManagerReplacement(ctx, l, recorded, r.options.WrapperVersion, r.reason)
}
func (r *startupRuntime) replacement(ctx context.Context, l StartupLayout, recorded Endpoint) (StartupReplacement, error) {
	if r.options.Source == nil {
		return nil, ErrStartupAuthorityUnavailable
	}
	cap, err := r.options.Source.Replacement(ctx, l, recorded, r.reason)
	if err != nil {
		return nil, &startupSourceError{cause: err}
	}
	if err := r.validateReplacement(ctx, l, recorded, cap); err != nil {
		return nil, err
	}
	return cap, nil
}
func lockStartup(ctx context.Context, mu *sync.Mutex) error {
	for !mu.TryLock() {
		timer := time.NewTimer(staleInstancePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		mu.Unlock()
		return err
	}
	return nil
}
