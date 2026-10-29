package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func legacyCodecFixtures(t *testing.T) []string {
	t.Helper()
	paths, e := filepath.Glob("testdata/legacy-records/*.json")
	if e != nil || len(paths) != 6 {
		t.Fatalf("fixtures unavailable: %v count=%d", e, len(paths))
	}
	return paths
}
func legacyCodecInput(t *testing.T, path string) LegacyRecordInput {
	t.Helper()
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var head struct {
		SessionID string `json:"session_id"`
	}
	if e = json.Unmarshal(raw, &head); e != nil {
		t.Fatal(e)
	}
	return legacyRecordImage(head.SessionID, raw)
}

// Actual original-schema archival images keep every legacy native/private field
// and byte on an unchanged merge. This is persistence equivalence, not permission
// to restore old private goal/accounting state into an Objective-only runtime.
func TestLegacyRecordCodecPreservesOriginalWireImages(t *testing.T) {
	for _, path := range legacyCodecFixtures(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			c := NewLegacyRecordCodec()
			in := legacyCodecInput(t, path)
			record, e := c.DecodeLegacy(context.Background(), in)
			if e != nil {
				t.Fatal(e)
			}
			raw, e := c.EncodeMerge(context.Background(), in, record)
			if e != nil || !bytes.Equal(raw, in.Bytes) {
				t.Fatalf("unchanged wire bytes lost: %v", e)
			}
			if e = c.AcknowledgeCommitted(context.Background(), in); e != nil {
				t.Fatal(e)
			}
			var image map[string]json.RawMessage
			if e = json.Unmarshal(in.Bytes, &image); e != nil {
				t.Fatal(e)
			}
			image["future_private_member"] = json.RawMessage(`{"keep":[1,"opaque"]}`)
			raw, e = json.MarshalIndent(image, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			in = legacyRecordImage(in.SessionID, append(raw, '\n'))
			record, e = c.DecodeLegacy(context.Background(), in)
			if e != nil {
				t.Fatal(e)
			}
			record.LivenessDetail = "updated observation"
			record.UpdatedAt = record.UpdatedAt.Add(time.Second)
			raw, e = c.EncodeMerge(context.Background(), in, record)
			if e != nil {
				t.Fatal(e)
			}
			var after map[string]json.RawMessage
			if e = json.Unmarshal(raw, &after); e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(compactLegacyJSON(image["future_private_member"]), compactLegacyJSON(after["future_private_member"])) || !bytes.Equal(compactLegacyJSON(image["provider_host"]), compactLegacyJSON(after["provider_host"])) {
				t.Fatal("metadata merge lost unknown or provider-private state")
			}
		})
	}
}

// Hash/session/owner/provider/state mismatches reject beside each valid image;
// no malformed image is acknowledged. Active old state requiring a real native
// accounting/owner supplier refuses explicitly before any provider restoration.
func TestLegacyRecordCodecRejectsInvalidAndUnsupportedActiveState(t *testing.T) {
	for _, path := range legacyCodecFixtures(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			c := NewLegacyRecordCodec()
			in := legacyCodecInput(t, path)
			if _, e := c.DecodeLegacy(context.Background(), in); e != nil {
				t.Fatal(e)
			}
			bad := in
			bad.SHA256 = "wrong"
			if _, e := c.DecodeLegacy(context.Background(), bad); !errors.Is(e, ErrLegacyViewStale) {
				t.Fatalf("hash rejection: %v", e)
			}
			if e := c.AcknowledgeCommitted(context.Background(), bad); !errors.Is(e, ErrLegacyViewStale) {
				t.Fatalf("bad ACK: %v", e)
			}
			for _, field := range []string{"session_id", "owner", "provider", "state"} {
				var image map[string]json.RawMessage
				_ = json.Unmarshal(in.Bytes, &image)
				switch field {
				case "owner":
					image[field] = json.RawMessage(`{"kind":"unknown","id":"primary"}`)
				default:
					image[field] = json.RawMessage(`"invalid"`)
				}
				raw, _ := json.Marshal(image)
				if _, e := c.DecodeLegacy(context.Background(), legacyRecordImage(in.SessionID, raw)); e == nil {
					t.Fatalf("accepted invalid %s", field)
				}
			}
			if filepath.Base(path) == "claude-initial.json" || filepath.Base(path) == "codex-initial.json" {
				var image map[string]json.RawMessage
				_ = json.Unmarshal(in.Bytes, &image)
				image["state"] = json.RawMessage(`"running"`)
				raw, _ := json.Marshal(image)
				if _, e := c.DecodeLegacy(context.Background(), legacyRecordImage(in.SessionID, raw)); !errors.Is(e, ErrHostCapabilityUnavailable) {
					t.Fatalf("old active private state was silently dropped: %v", e)
				}
			}
		})
	}
}

// The real host writer creates/reloads the original provider_host.opaque wire
// using a factory boundary fixture. No provider, credentials or service runs.
func TestLegacyRecordCodecIsActualHostWriterSupplier(t *testing.T) {
	dir := t.TempDir()
	codec := NewLegacyRecordCodec()
	runner := &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: RunnerSnapshot{Kind: "muse-pty", MusePTY: &MusePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: "native-fixture", PID: 123, LogPath: "/fixture/output.log"}}}
	layout := Layout{Namespace: "legacy-fixture", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "manager.lock")}
	h, e := NewHost(HostOptions{Layout: layout, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Codec: codec, Factories: []ProviderHostFactory{hostCommitProbeFactory{runner}}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	a, e := h.Attach(context.Background(), AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(layout.SessionsDir, a.Session.SessionID, "record.json"))
	if e != nil {
		t.Fatal(e)
	}
	var wire struct {
		ProviderHost struct {
			Provider string          `json:"provider"`
			Opaque   json.RawMessage `json:"opaque"`
		} `json:"provider_host"`
	}
	if e = json.Unmarshal(raw, &wire); e != nil || wire.ProviderHost.Provider != "muse" || len(wire.ProviderHost.Opaque) == 0 {
		t.Fatalf("surrogate or missing actual wire: %v", e)
	}
	if _, e = h.Session(context.Background(), a.Session.SessionID); e != nil {
		t.Fatal(e)
	}
	decoded, e := codec.DecodeLegacy(context.Background(), legacyRecordImage(a.Session.SessionID, raw))
	if e != nil || decoded.ProviderHost.Snapshot.MusePTY.SessionID != "native-fixture" {
		t.Fatalf("native identity changed: %v", e)
	}
}

// New cold records encode each original native format from complete typed facts.
// Legacy private initialization is not inferred or copied into a new session.
func TestLegacyRecordCodecInitialSixNativeShapes(t *testing.T) {
	for _, path := range legacyCodecFixtures(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			c := NewLegacyRecordCodec()
			in := legacyCodecInput(t, path)
			r, err := c.DecodeLegacy(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			r.SessionID = "SES-cold-codec-fixture-0001"
			r.State = SessionRunning
			r.AppliedGoalRef = nil
			r.OwnerRunError = ""
			// This is a newly initialized cold image, not restoration of the
			// archived native goal. Preserve physical observations while
			// explicitly supplying its new empty accounting initialization.
			if native := r.ProviderHost.Snapshot.MuseMSP; native != nil && native.GoalTransport == "native" {
				native.NativeAccounting = nil
			}
			raw, err := c.EncodeInitial(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.DecodeLegacy(context.Background(), legacyRecordImage(r.SessionID, raw))
			if err != nil || !reflect.DeepEqual(got, r) {
				t.Fatalf("cold native image mismatch: %v", err)
			}
		})
	}
}

// Transport discriminators cannot compete with PTY provenance. Exact neighboring
// native and PTY shapes decode normally; an injected mixed image never does.
func TestLegacyRecordCodecRefusesMixedMuseWireShapes(t *testing.T) {
	c := NewLegacyRecordCodec()
	in := legacyCodecInput(t, "testdata/legacy-records/muse-native-initial.json")
	if _, err := c.DecodeLegacy(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	var image map[string]json.RawMessage
	_ = json.Unmarshal(in.Bytes, &image)
	var host map[string]json.RawMessage
	_ = json.Unmarshal(image["provider_host"], &host)
	var opaque map[string]json.RawMessage
	_ = json.Unmarshal(host["opaque"], &opaque)
	opaque["profile"] = json.RawMessage(`"foreign-variant"`)
	host["opaque"], _ = json.Marshal(opaque)
	image["provider_host"], _ = json.Marshal(host)
	raw, _ := json.Marshal(image)
	if _, err := c.DecodeLegacy(context.Background(), legacyRecordImage(in.SessionID, raw)); err == nil {
		t.Fatal("mixed Muse legacy variants were admitted")
	}
}
