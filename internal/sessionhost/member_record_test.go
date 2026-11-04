package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recordFixture(t *testing.T) (map[string]any, []byte) {
	t.Helper()
	v := map[string]any{
		"schema": "werkstatt-record-v1", "id": "member-1", "project": "project", "role": "orchestrator", "runtime": "codex_cli",
		"model": nil, "effort": nil, "profile": "swarm-orch", "profile_lock_sha256": strings.Repeat("a", 64), "clone": "/workspace",
		"identity": "fixture.orch", "room": "!fixture:example.test", "orchestrator": "fixture.parent", "sandbox": "none", "trust_class": "operator", "doorbell": nil,
		"notification": map[string]any{"member": map[string]any{"mxid": "@fixture.orch:example.test", "agent_selector": "fixture.orch", "connection": "fixture-gr", "configuration_root": "/configured/source"}, "admission_reference": "opaque-assignment-v1", "call_timeout_ms": 1000},
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return v, b
}

func recordWrite(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func recordSourceFixture(t *testing.T) (*continuingHostOwner, *memberRecordSource, []byte) {
	t.Helper()
	cfg, _ := continuingOwnerConfig(t)
	owner := continuingOwnerOpen(t, context.Background(), time.Now().Add(30*time.Second), cfg)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, data := recordFixture(t)
	recordWrite(t, dir, "member-1.json", data)
	source, err := owner.openMemberRecordSource(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.close(); err != nil {
			t.Error(err)
		}
	})
	return owner, source, data
}

func retainRecord(t *testing.T, s *memberRecordSource) *retainedMemberRecord {
	t.Helper()
	r, err := s.retain("opaque-assignment-v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	return r
}

func recordNoAttachment(t *testing.T, o *continuingHostOwner) {
	t.Helper()
	o.host.mu.Lock()
	defer o.host.mu.Unlock()
	if o.host.notificationManager != nil || o.host.notificationStream != nil {
		t.Fatal("record observation created attachment effects")
	}
}

// Actual configured files, a retained directory and an actual local Host owner
// establish this source/revision conjunct. Expected JSON and decoded metadata do
// not establish full manager, Source, native or profile/workflow admission.
func TestMemberRecordRetainedOrigin(t *testing.T) {
	o, s, _ := recordSourceFixture(t)
	legacy, _ := recordFixture(t)
	legacy["id"] = "legacy"
	delete(legacy, "notification")
	delete(legacy, "profile_lock_sha256")
	data, _ := json.Marshal(legacy)
	recordWrite(t, s.path, "legacy.json", data)
	r := retainRecord(t, s)
	if err := r.recheck(o, s); err != nil {
		t.Fatal(err)
	}
	if r.record.ID != "member-1" || r.record.Notification.Member.Connection != "fixture-gr" || r.record.ProfileLock != strings.Repeat("a", 64) {
		t.Fatal("wrong actual observation")
	}
	other, err := o.openMemberRecordSource(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.close()
	if err := r.recheck(o, other); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("foreign source admitted", err)
	}
	foreign := &continuingHostOwner{host: o.host, ctx: o.ctx}
	if err := r.recheck(foreign, s); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("foreign owner admitted", err)
	}
	if err := r.recheck(o, s); err != nil {
		t.Fatal("foreign query damaged actual source", err)
	}
	recordNoAttachment(t, o)
	if _, ok := any(r).(notificationManagerCustody); ok {
		t.Fatal("record observation granted full custody")
	}
}

// Each observed loss is terminal even after restoring matching JSON. Fresh
// nearby fixtures supply the positive control; no broker/Source/lease is opened.
func TestMemberRecordCurrentOrigin(t *testing.T) {
	cases := map[string]func(*testing.T, *memberRecordSource, []byte){
		"identical-file-replacement": func(t *testing.T, s *memberRecordSource, b []byte) {
			// Keep the old inode linked: only identity, not unlink detection or
			// a byte mismatch, can distinguish these two legitimate files.
			if err := os.Rename(filepath.Join(s.path, "member-1.json"), filepath.Join(s.path, "retained-original")); err != nil {
				t.Fatal(err)
			}
			recordWrite(t, s.path, "replacement", b)
			if err := os.Rename(filepath.Join(s.path, "replacement"), filepath.Join(s.path, "member-1.json")); err != nil {
				t.Fatal(err)
			}
		},
		"in-place-revision": func(t *testing.T, s *memberRecordSource, b []byte) {
			recordWrite(t, s.path, "member-1.json", append(b, ' '))
		},
		"missing": func(t *testing.T, s *memberRecordSource, b []byte) {
			if err := os.Remove(filepath.Join(s.path, "member-1.json")); err != nil {
				t.Fatal(err)
			}
		},
		"duplicate-reference": func(t *testing.T, s *memberRecordSource, b []byte) {
			recordWrite(t, s.path, "member-2.json", []byte(strings.Replace(string(b), `"id":"member-1"`, `"id":"member-2"`, 1)))
		},
		"file-permissions": func(t *testing.T, s *memberRecordSource, b []byte) {
			if err := os.Chmod(filepath.Join(s.path, "member-1.json"), 0666); err != nil {
				t.Fatal(err)
			}
		},
		"directory-permissions": func(t *testing.T, s *memberRecordSource, b []byte) {
			if err := os.Chmod(s.path, 0777); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, s *memberRecordSource, b []byte) {
			if err := os.Rename(filepath.Join(s.path, "member-1.json"), filepath.Join(s.path, "original")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("original", filepath.Join(s.path, "member-1.json")); err != nil {
				t.Fatal(err)
			}
		},
		"directory-replacement": func(t *testing.T, s *memberRecordSource, b []byte) {
			old := s.path + "-old"
			if err := os.Rename(s.path, old); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(old) })
			if err := os.Mkdir(s.path, 0700); err != nil {
				t.Fatal(err)
			}
			recordWrite(t, s.path, "member-1.json", b)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			o, s, data := recordSourceFixture(t)
			r := retainRecord(t, s)
			if err := r.recheck(o, s); err != nil {
				t.Fatal(err)
			}
			change(t, s, data)
			if err := r.recheck(o, s); !errors.Is(err, errMemberRecordUnavailable) {
				t.Fatal("substituted origin was admitted", err)
			}
			if !r.lost {
				t.Fatal("observed loss was not terminal")
			}
			recordNoAttachment(t, o)
		})
	}
}

// The lookup scans actual configured records and refuses zero/multiple/stale
// references before creating any attachment. It never uses a request path.
func TestMemberRecordReferenceResolution(t *testing.T) {
	for _, name := range []string{"missing", "empty", "duplicate", "id-mismatch", "hardlink"} {
		t.Run(name, func(t *testing.T) {
			o, s, data := recordSourceFixture(t)
			reference := "opaque-assignment-v1"
			switch name {
			case "missing":
				reference = "absent-reference"
			case "empty":
				reference = ""
			case "duplicate":
				recordWrite(t, s.path, "member-2.json", []byte(strings.Replace(string(data), `"id":"member-1"`, `"id":"member-2"`, 1)))
			case "id-mismatch":
				recordWrite(t, s.path, "member-2.json", data)
			case "hardlink":
				if err := os.Link(filepath.Join(s.path, "member-1.json"), filepath.Join(s.path, "alias")); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.retain(reference)
			if r != nil || !errors.Is(err, errMemberRecordUnavailable) {
				if r != nil {
					r.close()
				}
				t.Fatal("unavailable reference admitted", err)
			}
			recordNoAttachment(t, o)
		})
	}
}

// The published baseline schema preserves legitimate nullable/optional fields.
// New expected fields are closed and strict. Unknown keys, escaped duplicates,
// invalid UTF-8/surrogates and noncanonical integers cannot be repaired or lost
// when separating the new optional member from the original record schema.
func TestMemberRecordInputCompatibility(t *testing.T) {
	_, good := recordFixture(t)
	for _, name := range []string{"minimal", "legacy", "full", "multibyte-reference"} {
		t.Run(name, func(t *testing.T) {
			v, _ := recordFixture(t)
			switch name {
			case "legacy":
				delete(v, "notification")
				delete(v, "profile_lock_sha256")
			case "full":
				v["git_dir"] = "/workspace/.git"
				v["model"] = "model"
				v["effort"] = "high"
				v["context_mode"] = "fresh"
				v["harness_memory"] = false
				v["child_env"] = map[string]string{"FIXTURE": "value"}
				v["config_decision_id"] = "decision-1"
				v["doorbell"] = map[string]any{"heartbeat": "30s", "max_wakes_per_hour": 3}
			case "multibyte-reference":
				v["notification"].(map[string]any)["admission_reference"] = strings.Repeat("é", 256)
			}
			b, _ := json.Marshal(v)
			if _, err := parseMemberRecord(b); err != nil {
				t.Fatal("legitimate form refused", err)
			}
		})
	}
	nullRecord, _ := recordFixture(t)
	nullRecord["notification"] = nil
	nullInput, _ := json.Marshal(nullRecord)
	bad := map[string][]byte{
		"duplicate":        []byte(strings.Replace(string(good), `"id":"member-1"`, `"id":"member-1","\u0069d":"member-1"`, 1)),
		"unknown-baseline": []byte(strings.Replace(string(good), `"schema":`, `"sig":"ignored?","schema":`, 1)),
		"unknown-input":    []byte(strings.Replace(string(good), `"call_timeout_ms":1000`, `"extra":true,"call_timeout_ms":1000`, 1)),
		"missing-timeout":  []byte(strings.Replace(string(good), `"call_timeout_ms":1000,`, "", 1)),
		"null-input":       nullInput,
		"null-timeout":     []byte(strings.Replace(string(good), `"call_timeout_ms":1000`, `"call_timeout_ms":null`, 1)),
		"fraction":         []byte(strings.Replace(string(good), `"call_timeout_ms":1000`, `"call_timeout_ms":1000.0`, 1)),
		"exponent":         []byte(strings.Replace(string(good), `"call_timeout_ms":1000`, `"call_timeout_ms":1e3`, 1)),
		"too-large":        []byte(strings.Replace(string(good), `"call_timeout_ms":1000`, `"call_timeout_ms":60001`, 1)),
		"wrong-case":       []byte(strings.Replace(string(good), `"mxid":`, `"MXID":`, 1)),
		"foreign-agent":    []byte(strings.Replace(string(good), `"agent_selector":"fixture.orch"`, `"agent_selector":"other.orch"`, 1)),
		"surrogate":        []byte(strings.Replace(string(good), `opaque-assignment-v1`, `\ud800`, 1)),
		"utf8":             bytesWithInvalidUTF8(good),
		"missing-lock":     []byte(strings.Replace(string(good), `"profile_lock_sha256":"`+strings.Repeat("a", 64)+`",`, "", 1)),
		"byte-limit":       []byte(strings.Replace(string(good), `opaque-assignment-v1`, strings.Repeat("é", 257), 1)),
	}
	for name, b := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMemberRecord(b); !errors.Is(err, errMemberRecordUnavailable) {
				t.Fatal("forbidden input accepted", err)
			}
		})
	}
}

func bytesWithInvalidUTF8(b []byte) []byte {
	return []byte(strings.Replace(string(b), "opaque-assignment-v1", string([]byte{0xff}), 1))
}

// Owner cancellation closes retained source/file handles without touching the
// inert runner. A source cannot be reopened through a dead or fabricated owner.
func TestMemberRecordOwnerLifetime(t *testing.T) {
	o, s, _ := recordSourceFixture(t)
	r := retainRecord(t, s)
	wait, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := o.close(wait); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.done:
	case <-wait.Done():
		t.Fatal("source handles were not closed")
	}
	if err := r.recheck(o, s); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("dead owner admitted", err)
	}
	s.mu.Lock()
	held := r.file
	s.mu.Unlock()
	if held != nil {
		t.Fatal("retained file leaked after owner death")
	}
	if again, err := o.openMemberRecordSource(s.path); again != nil || !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("dead owner opened source", err)
	}
	recordNoAttachment(t, o)
}

// Restoring a current source does not resurrect an already rejected retained
// revision. A fresh lookup proves the restored fixture is independently valid.
func TestMemberRecordLossIsTerminal(t *testing.T) {
	o, s, data := recordSourceFixture(t)
	r := retainRecord(t, s)
	recordWrite(t, s.path, "member-1.json", append(data, ' '))
	if err := r.recheck(o, s); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("changed revision admitted", err)
	}
	recordWrite(t, s.path, "member-1.json", data)
	fresh := retainRecord(t, s)
	if err := fresh.recheck(o, s); err != nil {
		t.Fatal("restored positive refused", err)
	}
	if err := r.recheck(o, s); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("lost record revived", err)
	}
	recordNoAttachment(t, o)
}

// Only the composition-selected canonical directory is opened. Missing,
// symlinked, relative and writable sources cannot become retained origins.
func TestMemberRecordSourceSelection(t *testing.T) {
	o, s, _ := recordSourceFixture(t)
	alias := s.path + "-alias"
	if err := os.Symlink(s.path, alias); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(alias) })
	for _, path := range []string{"", "relative", s.path + "/missing", alias, s.path + "/../" + filepath.Base(s.path)} {
		if got, err := o.openMemberRecordSource(path); got != nil || !errors.Is(err, errMemberRecordUnavailable) {
			if got != nil {
				_ = got.close()
			}
			t.Fatal("foreign source selected", err)
		}
	}
	if err := os.Chmod(s.path, 0777); err != nil {
		t.Fatal(err)
	}
	if got, err := o.openMemberRecordSource(s.path); got != nil || !errors.Is(err, errMemberRecordUnavailable) {
		if got != nil {
			_ = got.close()
		}
		t.Fatal("writable source selected", err)
	}
	recordNoAttachment(t, o)
}
