package sessionplugin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

// ResumeHost reuses an admitted native conversation. ValidateResume is read-only
// and runs before profile comparison, reservation or any provider invocation.
type ResumeHost interface {
	ValidateResume(context.Context, string) error
	Resume(context.Context, string, Request) (HostSession, error)
}

// ControlHost transports only manager-private attachment data. It is never a
// public JSON reply; the terminal CLI consumes it over authenticated local RPC.
type ControlHost interface {
	Stop(context.Context, string, string) (HostSession, error)
	Attach(context.Context, string) (json.RawMessage, error)
}

// ConditionalControlHost performs the busy check and turn fence atomically in
// the provider host; projecting an idle row is never sufficient authority.
type ConditionalControlHost interface {
	StopNotBusy(context.Context, string, string) (HostSession, error)
}

func currentSessions(s *ledger) map[string]*storedSession {
	rows := map[string]*storedSession{}
	for _, p := range s.Operations {
		if p.Session == nil {
			continue
		}
		r := p.Session
		if old := rows[r.Record.Session]; old == nil || *old.Record.Generation < *r.Record.Generation {
			rows[r.Record.Session] = r
		}
	}
	return rows
}
func canonicalCWD(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}
func (e Engine) selectResume(ctx context.Context, s *ledger, q Request) (*storedSession, error) {
	rows := currentSessions(s)
	matches := []*storedSession{}
	unreadable := false
	for _, r := range rows {
		if r.Record.Runtime == nil || *r.Record.Runtime != q.Runtime {
			continue
		}
		projected := e.project(ctx, r)
		if q.Selector.Kind == "handle" && r.Record.Session != q.Selector.Handle {
			continue
		}
		native := projected.NativeID
		if native == nil {
			native = r.Record.NativeID
		}
		if q.Selector.Kind == "provider_id" && (native == nil || *native != q.Selector.ID) {
			continue
		}
		if canonicalCWD(r.CWD) != canonicalCWD(q.Scope.CWD) || !labelsMatch(r.Record.Labels, q.Scope.Labels) {
			if q.Selector.Kind == "handle" {
				return nil, Fail("session_resume_location_mismatch")
			}
			continue
		}
		if !projected.Readable {
			if q.Selector.Kind == "handle" {
				return nil, Fail("session_resume_unreadable")
			}
			unreadable = true
			continue
		}
		matches = append(matches, r)
	}
	if unreadable {
		return nil, Fail("session_resume_ambiguous")
	}
	if len(matches) == 0 {
		return nil, Fail("session_resume_not_found")
	}
	sort.Slice(matches, func(i, j int) bool { return *matches[i].Record.StartedAt > *matches[j].Record.StartedAt })
	if len(matches) > 1 && (q.Selector.Kind != "latest" || *matches[0].Record.StartedAt == *matches[1].Record.StartedAt) {
		return nil, Fail("session_resume_ambiguous")
	}
	selected := matches[0]
	host, ok := e.Host.(ResumeHost)
	if !ok || !selected.Binding {
		return nil, Fail("session_resume_unreadable")
	}
	if err := host.ValidateResume(ctx, selected.Target); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(q.Profile, selected.Profile) {
		return nil, Fail("session_resume_identity_mismatch")
	}
	state := e.project(ctx, selected)
	if !state.Readable {
		return nil, Fail("session_resume_unreadable")
	}
	if state.State != "down" {
		return nil, Fail("werkstatt_already_up")
	}
	return selected, nil
}
func recordEnvelope(r Record) map[string]any {
	raw, _ := json.Marshal(r)
	var v map[string]any
	_ = json.Unmarshal(raw, &v)
	return Envelope(v)
}
func (e Engine) control(ctx context.Context, op string, q Request) (string, any, error) {
	var value any
	name := "session"
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err := e.transactionContext(ctx, func(s *ledger) (bool, error) {
		selected := currentSessions(s)[q.Session]
		if selected == nil {
			return false, Fail("session_host_unavailable")
		}
		// A resume can have started its provider before publishing its record.
		// Never let an observation of the old generation control that child.
		for _, pending := range s.Operations {
			if pending.Resume != nil && pending.Resume.Target == selected.Target && (pending.State == "admitted" || pending.State == "unknown") {
				return false, Fail("session_generation_mismatch")
			}
		}
		if *selected.Record.Generation != q.ExpectedGeneration {
			return false, Fail("session_generation_mismatch")
		}
		row := e.project(ctx, selected)
		if !row.Readable {
			return false, Fail("session_host_unavailable")
		}
		host, ok := e.Host.(ControlHost)
		if !ok {
			return false, unavailable()
		}
		if op == "attach" {
			if selected.Detached || row.State == "down" || row.State == "draining" {
				return false, Fail("session_host_unavailable")
			}
			var err error
			value, err = host.Attach(ctx, selected.Target)
			name = "attachment"
			return false, err
		}
		// This fast refusal has no effects. A projected idle still requires the
		// provider-owned atomic check/fence below; it is only an observation.
		if q.Precondition == "not-busy" && row.State != "idle" && row.State != "down" {
			return false, Fail("werkstatt_busy", map[string]string{"state": "busy"})
		}
		if row.State != "down" {
			var err error
			if q.Precondition == "not-busy" {
				conditional, ok := e.Host.(ConditionalControlHost)
				if !ok {
					return false, Fail("werkstatt_busy", map[string]string{"state": "busy"})
				}
				_, err = conditional.StopNotBusy(ctx, selected.Target, q.Mode)
			} else {
				_, err = host.Stop(ctx, selected.Target, q.Mode)
			}
			if err != nil {
				return false, err
			}
		}
		value = recordEnvelope(e.project(ctx, selected))
		return false, nil
	})
	return name, value, err
}
