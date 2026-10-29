//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

func standaloneOptions(t *testing.T) StandaloneOptions {
	t.Helper()
	root := t.TempDir()
	layout, err := LayoutAt(root, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return StandaloneOptions{Layout: layout, Config: FileConfig{Path: filepath.Join(root, "optional.json"), Optional: true, Version: "fixture", Namespace: layout.Namespace}, GoalDirectory: filepath.Join(root, "goals"), BudgetDirectory: filepath.Join(root, "accounting")}
}

// The new core status reads actual service inventory, including raw malformed
// materialization quarantine. A missing legacy validator is explicitly absent;
// the original descriptor is never advertised from data alone.
func TestStandaloneStatusUsesActualContextInventory(t *testing.T) {
	o := standaloneOptions(t)
	s, err := OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Control.Status(); got.MaterializationCount != 0 || got.QuarantinedMaterializationCount != 0 || got.GoalReflectionContract != nil {
		t.Fatalf("initial actual inventory: %+v", got)
	}
	// Ordinary service files must not look like a lost controller generation
	// when no provider session has yet been admitted.
	if err := os.MkdirAll(o.Layout.LogsDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(o.Layout.OperatorLogFile, []byte("fixture startup record\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := "CTX-261029-c00001"
	dir := filepath.Join(o.Layout.MaterializationsDir, id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"malformed":`)
	if err := os.WriteFile(filepath.Join(dir, "record.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	got := s.Control.Status()
	if got.MaterializationCount != len(s.Contexts.ListMaterializations()) || got.QuarantinedMaterializationCount != 1 || got.GoalReflectionContract != nil {
		t.Fatalf("catalog/quarantine source lost: %+v", got)
	}
	retained, err := os.ReadFile(filepath.Join(o.Layout.MaterializationQuarantine, id, "record.json"))
	if err != nil || string(retained) != string(raw) {
		t.Fatalf("quarantine changed raw custody: %v", err)
	}
}

// This is an actual component constructor and in-process handler test. It does
// not start a daemon, native provider, live authority or inspect credentials.
// It proves durable reopen, same factory binding, real original context dispatch
// and preserved fail-closed routes in the absent external-source default.
func TestStandaloneComposesActualServicesAndReopens(t *testing.T) {
	o := standaloneOptions(t)
	s, err := OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	if s.Host == nil || s.Goals == nil || s.Contexts == nil || s.OwnerRuns == nil || s.Budget == nil || s.Control == nil {
		t.Fatal("actual composition missing")
	}
	if err := s.OwnerRuns.BindHost(s.Host); !errors.Is(err, core.ErrInvalidInput) {
		t.Fatalf("host was not bound exactly once: %v", err)
	}
	if err := s.OwnerRuns.BindBudget(s.Budget); !errors.Is(err, core.ErrInvalidInput) {
		t.Fatalf("same budget was not already bound: %v", err)
	}
	h, err := s.controlHandler(s.Control.Status)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method, path, body, want string
		status                   int
	}{
		{"GET", "/v1/status", "", `"protocol_version":8`, 200},
		{"GET", "/v1/sessions", "", `"sessions":[]`, 200},
		{"GET", "/v1/contexts/materializations", "", `"materializations":[]`, 200},
		{"POST", "/v1/contexts/completeness", `{"unknown":true}`, `"code":"invalid_request"`, 400},
		{"POST", "/v1/owners/reconcile", `{"owner":{"kind":"parent","id":"primary"}}`, `"code":"reconcile_failed"`, 409},
		{"POST", "/v1/sessions/attach", `{}`, `"code":"attach_failed"`, 409},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
		if w.Code != test.status || !strings.Contains(w.Body.String(), test.want) {
			t.Fatalf("actual route %s: %d %s", test.path, w.Code, w.Body)
		}
	}
	// All eight context POST routes reach the real strict decoder. Unknown
	// fields refuse before service effects; GET inventory above is route nine.
	for _, suffix := range []string{"reclaim", "sweep", "economics", "completeness", "materializations/prepare", "materializations/complete", "materializations/fail", "builder-gateway/finalize"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/v1/contexts/"+suffix, strings.NewReader(`{"unknown":true}`)))
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("context dispatch %s: %d %s", suffix, w.Code, w.Body)
		}
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatalf("reopen existing journals: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Invalid layout, mismatched namespace, missing explicit config, cancellation
// and invalid retention refuse before creating durable host/goal/budget files.
// The real valid constructor above prevents reject-all qualification.
func TestStandaloneRefusesBeforeRuntimeEffects(t *testing.T) {
	for _, kind := range []string{"relative-goals", "namespace", "selected-config", "cancelled", "retention"} {
		t.Run(kind, func(t *testing.T) {
			o := standaloneOptions(t)
			ctx := context.Background()
			want := error(nil)
			switch kind {
			case "relative-goals":
				o.GoalDirectory = "relative"
			case "namespace":
				o.Config.Namespace = "foreign"
			case "selected-config":
				o.Config.Optional = false
			case "cancelled":
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(ctx)
				stop()
				want = context.Canceled
			case "retention":
				o.Config.Optional = false
				if err := os.WriteFile(o.Config.Path, []byte(`{"contexts":{"retention":{"ready_ttl":"0s"}}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s, err := OpenStandalone(ctx, o)
			if err == nil || s != nil || want != nil && !errors.Is(err, want) {
				t.Fatalf("invalid %s accepted: %v", kind, err)
			}
			if kind == "retention" && !strings.Contains(err.Error(), "ready_ttl") {
				t.Fatalf("wrong retention refusal: %v", err)
			}
			for _, p := range []string{o.Layout.StateDir, o.BudgetDirectory, filepath.Join(filepath.Dir(o.Layout.StateDir), "goals")} {
				if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refusal wrote %s: %v", p, err)
				}
			}
		})
	}
}

// Layout is storage only. Relative roots and blank namespaces cannot become a
// runtime; distinct namespace strings do not create identity/grant suppliers.
func TestStandaloneLayoutRequiresExplicitLocalFacts(t *testing.T) {
	for _, test := range []struct {
		root      string
		namespace core.NamespaceKey
	}{{"relative", "x"}, {t.TempDir(), ""}, {t.TempDir(), " bad "}} {
		if _, err := LayoutAt(test.root, test.namespace); err == nil {
			t.Fatal("invalid layout accepted")
		}
	}
	l, err := LayoutAt(t.TempDir(), "selected")
	if err != nil || l.Namespace != "selected" || filepath.Base(l.LockFile) != "manager.lock" {
		t.Fatalf("valid explicit layout: %+v %v", l, err)
	}
}
