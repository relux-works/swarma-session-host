package sessioncontext

import (
	"context"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"reflect"
	"strings"
	"testing"
	"time"
)

type sourceValidationFixture struct {
	decision                                 func(*sm.ContextForkPreflightDecision)
	boundary                                 func(*sm.ContextBoundaryBanner)
	policyErr                                error
	preflightErr                             error
	bannerErr                                error
	policyCalls, preflightCalls, bannerCalls int
	saw                                      sm.ContextForkPreflight
}

func (f *sourceValidationFixture) ActiveContextValidationPolicy(context.Context) (sm.ContextValidationPolicy, error) {
	f.policyCalls++
	return sm.ContextValidationPolicy{ValidatorVersion: "source-validator", ExtractorVersion: "source-extractor"}, f.policyErr
}
func (f *sourceValidationFixture) PreflightContextFork(_ context.Context, r sm.ContextForkPreflight) (*sm.ContextForkPreflightDecision, error) {
	f.preflightCalls++
	f.saw = r
	d := &sm.ContextForkPreflightDecision{ContractVersion: "context-validation-lifecycle-v1", CtxID: r.Record.ID, Mode: r.Mode, State: r.Record.State, Admitted: true, Fallback: "none"}
	if f.decision != nil {
		f.decision(d)
	}
	return d, f.preflightErr
}
func (f *sourceValidationFixture) BuildContextBoundaryBanner(_ context.Context, r *sm.ContextRecord, m *sm.CoverageManifest, ref string, _ time.Time) (*sm.ContextBoundaryBanner, error) {
	f.bannerCalls++
	b := &sm.ContextBoundaryBanner{ContractVersion: "context-boundary-banner-v1", CtxID: r.ID, ManifestRef: ref, AnchorCommit: r.Freshness.CommitSHA, ValidationAge: "2h", CoveredReads: []string{"src/actual.go"}, CoveredReadCount: 1, UnknownRemainder: true}
	if f.boundary != nil {
		f.boundary(b)
	}
	return b, f.bannerErr
}

type capturedMaterializer struct {
	calls    int
	received ForkPreparation
}

func (f *capturedMaterializer) Prepare(_ context.Context, p ForkPreparation) (sm.ProviderHostRecord, error) {
	f.calls++
	f.received = p
	return testMaterializationHost(), nil
}
func forkServiceFixture(t *testing.T) (*Service, *fakeMaterializationAuthority, *sourceValidationFixture, *capturedMaterializer, PrepareRunMaterializationRequest, []byte) {
	t.Helper()
	primary := materializationTestContext("CTX-260729-c04201", true)
	primary.State = "ready"
	primary.SecretClass = "unrestricted"
	primary.Payload.SHA256 = strings.Repeat("a", 64)
	child := materializationTestContext("CTX-260729-c04202", true)
	child.SecretClass = "unrestricted"
	child.Lineage.PrimaryCtxID = primary.ID
	child.Lineage.ProducedByRunID = "RUN-fork-validation"
	a := newFakeMaterializationAuthority(primary, child)
	a.runs[child.Lineage.ProducedByRunID] = &sm.RunEvidence{ID: child.Lineage.ProducedByRunID, ContextID: child.ID, Owner: sm.OwnerKey(child.Binding.ElementID), Status: "running"}
	a.manifests[primary.ID] = &sm.CoverageManifest{CtxID: primary.ID, Provider: primary.Provider, ManifestSHA256: strings.Repeat("b", 64)}
	m := newMaterializationManager(t, testLayout(t, "actual-validation-port"), a)
	if _, e := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: primary, RunProvider: "codex", TargetElementID: primary.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: testMaterializationHost(), PayloadSHA256: primary.Payload.SHA256}); e != nil {
		t.Fatal(e)
	}
	f := &sourceValidationFixture{}
	p := &capturedMaterializer{}
	m.validation = f
	m.providerMaterializers = map[sm.ContextProvider]ProviderMaterializer{"codex": p}
	return m, a, f, p, PrepareRunMaterializationRequest{Context: child, PrimaryCtxID: primary.ID, TargetElementID: child.Binding.ElementID, TargetRole: "developer", RunID: child.Lineage.ProducedByRunID, Mode: "explicit"}, catalogBytes(t, m.layout, primary.ID)
}

// The real typed validation port is called with current source facts and policy;
// the actual source-built Render reaches provider preparation unchanged. This
// fixture proves the data boundary, not native provider availability or a
// production authority's validation algorithm.
func TestContextForkActualValidationPortCarriesDecisionAndBoundary(t *testing.T) {
	m, _, f, p, r, _ := forkServiceFixture(t)
	v, e := m.PrepareRunMaterialization(context.Background(), r)
	if e != nil || v.State != MaterializationLeased || v.ActiveRunID != r.RunID {
		t.Fatalf("admitted data preparation failed: %+v %v", v, e)
	}
	if f.policyCalls != 1 || f.preflightCalls != 1 || f.bannerCalls != 1 || f.saw.Policy.ValidatorVersion != "source-validator" || f.saw.Record.ID != r.PrimaryCtxID || !f.saw.Now.Equal(materializationTestClock) {
		t.Fatal("actual current-policy source port bypassed")
	}
	prep := p.received
	if p.calls != 1 || prep.Decision == nil || !prep.Decision.Admitted || prep.Banner == nil || prep.Rendered != prep.Banner.Render() || !strings.Contains(prep.Rendered, "src/actual.go") || !strings.Contains(prep.Rendered, "UNKNOWN") || prep.Banner.ManifestRef != r.PrimaryCtxID+"_coverage-manifest.json" {
		t.Fatal("source decision/boundary payload dropped or reconstructed")
	}
}

// Refusal, foreign identity, stale source, missing evidence and malformed source
// boundaries all stop before provider effects and leave the original catalog
// intact. A security permit alone is never a validation decision.
func TestContextForkValidationRefusalsBeforeProviderEffects(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Service, *fakeMaterializationAuthority, *sourceValidationFixture, *PrepareRunMaterializationRequest)
	}{
		{"source-mirror-drift", func(_ *Service, a *fakeMaterializationAuthority, _ *sourceValidationFixture, r *PrepareRunMaterializationRequest) {
			a.contexts[r.PrimaryCtxID].Payload.SHA256 = strings.Repeat("c", 64)
		}},
		{"missing", func(m *Service, _ *fakeMaterializationAuthority, _ *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			m.validation = nil
		}},
		{"policy-error", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.policyErr = errors.New("source policy unavailable")
		}},
		{"preflight-error", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.preflightErr = errors.New("source preflight unavailable")
		}},
		{"refused", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.decision = func(d *sm.ContextForkPreflightDecision) {
				d.Admitted = false
				d.Reasons = []sm.ContextValidationReason{"validation_evidence_unreadable"}
				d.Fallback = "fail"
			}
		}},
		{"foreign-id", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.decision = func(d *sm.ContextForkPreflightDecision) { d.CtxID = "CTX-260729-other1" }
		}},
		{"wrong-mode", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.decision = func(d *sm.ContextForkPreflightDecision) { d.Mode = "auto" }
		}},
		{"skipped", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.decision = func(d *sm.ContextForkPreflightDecision) { d.Skipped = true }
		}},
		{"model-required", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.decision = func(d *sm.ContextForkPreflightDecision) { d.ModelAssayRequired = true }
		}},
		{"boundary-error", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.bannerErr = errors.New("coverage unavailable")
		}},
		{"foreign-boundary", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.boundary = func(b *sm.ContextBoundaryBanner) { b.CtxID = "CTX-260729-other1" }
		}},
		{"wrong-resource", func(_ *Service, _ *fakeMaterializationAuthority, f *sourceValidationFixture, _ *PrepareRunMaterializationRequest) {
			f.boundary = func(b *sm.ContextBoundaryBanner) { b.ManifestRef = "digest-only" }
		}},
		{"forged-lineage", func(_ *Service, _ *fakeMaterializationAuthority, _ *sourceValidationFixture, r *PrepareRunMaterializationRequest) {
			r.Context.Lineage.SupersedesCtxID = "CTX-260729-other1"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, a, f, p, r, before := forkServiceFixture(t)
			test.change(m, a, f, &r)
			_, e := m.PrepareRunMaterialization(context.Background(), r)
			if test.name == "refused" && !isContextErrorCode(e, "context_integrity_unverified") {
				t.Fatalf("original source refusal code changed: %v", e)
			}
			if test.name == "source-mirror-drift" && (e == nil || !strings.Contains(e.Error(), "payload digest does not match")) {
				t.Fatalf("exact primary mirror refusal missing: %v", e)
			}
			if e == nil || p.calls != 0 {
				t.Fatalf("refused source reached provider effects: %v calls=%d", e, p.calls)
			}
			if len(m.ListMaterializations()) != 1 || !reflect.DeepEqual(before, catalogBytes(t, m.layout, r.PrimaryCtxID)) {
				t.Fatal("refused source changed original catalog")
			}
		})
	}
}
