package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// LegacyRecordCodec realizes the original record persistence format. It has no
// filesystem/native capability, canonical ledger or private acknowledgement
// queue. codecRecords owns the actual write/readback/commit transaction.
type LegacyRecordCodec struct{ museAccounting MuseAccountingRestorer }

var _ ForkRunnerCodec = (*LegacyRecordCodec)(nil)

func NewLegacyRecordCodec() *LegacyRecordCodec { return &LegacyRecordCodec{} }

// These two fields are immutable legacy slot data, never caller authorization.
type legacySessionOwner struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// LegacyGoalRef retains the published native legacy reference shape.
type LegacyGoalRef struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}

type legacyGoalRef = LegacyGoalRef

var legacyRunOwnerPattern = regexp.MustCompile(`^RUN-[A-Za-z0-9][A-Za-z0-9-]*$`)
var legacyGoalRefPattern = regexp.MustCompile(`^(?:PRIMARY-)?GOAL-[A-Z0-9]+(?:-[A-Z0-9]+)+$`)

func validateLegacyGoalRef(r legacyGoalRef) error {
	if r.Revision < 1 || !legacyGoalRefPattern.MatchString(strings.ToUpper(strings.TrimSpace(r.ID))) {
		return fmt.Errorf("invalid legacy goal reference")
	}
	return nil
}
func (o legacySessionOwner) key() (OwnerKey, error) {
	id := strings.TrimSpace(o.ID)
	switch o.Kind {
	case "parent":
		if id != "primary" {
			return "", fmt.Errorf("invalid legacy parent owner")
		}
	case "run":
		if !legacyRunOwnerPattern.MatchString(id) {
			return "", fmt.Errorf("invalid legacy run owner")
		}
		id = strings.ToUpper(id)
	default:
		return "", fmt.Errorf("invalid legacy owner kind")
	}
	return OwnerKey(o.Kind + "/" + id), nil
}
func legacyOwnerForKey(key OwnerKey) (legacySessionOwner, error) {
	parts := strings.SplitN(string(key), "/", 2)
	if len(parts) != 2 {
		return legacySessionOwner{}, fmt.Errorf("%w: legacy owner slot required", ErrHostCapabilityUnavailable)
	}
	o := legacySessionOwner{parts[0], parts[1]}
	actual, e := o.key()
	if e != nil || actual != key {
		return legacySessionOwner{}, fmt.Errorf("invalid normalized legacy owner slot")
	}
	return o, nil
}

func decodeLegacyJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	if e := d.Decode(out); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return fmt.Errorf("one legacy JSON value required")
	}
	return nil
}
func compactLegacyJSON(raw []byte) []byte {
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return raw
	}
	return b.Bytes()
}
func codecLegacyInput(in LegacyRecordInput) error {
	if !validLegacySessionID(in.SessionID) || legacyRecordImage(in.SessionID, in.Bytes).SHA256 != in.SHA256 {
		return ErrLegacyViewStale
	}
	return nil
}
func (c *LegacyRecordCodec) DecodeLegacy(ctx context.Context, in LegacyRecordInput) (RunnerRecord, error) {
	if e := ctx.Err(); e != nil {
		return RunnerRecord{}, e
	}
	if e := codecLegacyInput(in); e != nil {
		return RunnerRecord{}, e
	}
	return c.decodeRecord(ctx, in.SessionID, in.Bytes, 0, false)
}
func (c *LegacyRecordCodec) decodeRecord(ctx context.Context, id string, raw []byte, depth int, archive bool) (RunnerRecord, error) {
	if e := ctx.Err(); e != nil {
		return RunnerRecord{}, e
	}
	if depth > 256 {
		return RunnerRecord{}, fmt.Errorf("legacy recovery chain too deep")
	}
	var wire legacyRunnerRecord
	if e := decodeLegacyJSON(raw, &wire); e != nil {
		return RunnerRecord{}, e
	}
	owner, e := wire.Owner.key()
	if e != nil {
		return RunnerRecord{}, e
	}
	if wire.SessionID != id || wire.ProviderHost.Provider != wire.Provider {
		return RunnerRecord{}, ErrLegacyViewStale
	}
	archive = archive || wire.State == SessionStopped
	snapshot, e := c.decodeSnapshot(wire.ProviderHost, archive)
	if e != nil {
		return RunnerRecord{}, e
	}
	out := RunnerRecord{GracefulRetirement: wire.GracefulRetirement, Contract: wire.Contract, SessionID: id, Owner: owner, Provider: wire.Provider, ProjectDir: wire.ProjectDir, NamespaceFingerprint: wire.BoardFingerprint, State: wire.State, Liveness: wire.Liveness, LivenessDetail: wire.LivenessDetail, OwnerRunError: wire.OwnerRunError, Eviction: wire.Eviction, ProviderHost: ProviderHostRecord{Provider: wire.Provider, Snapshot: snapshot}, MaterializationCtxID: wire.MaterializationCtxID, ProviderHostPendingFork: wire.ProviderHostPendingFork, CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt}
	if wire.AppliedGoalRef != nil {
		out.AppliedGoalRef = &RevisionRef{ID: wire.AppliedGoalRef.ID, Revision: wire.AppliedGoalRef.Revision}
	}
	if wire.ConditionalStop != nil {
		p, e := c.decodeRecord(ctx, id, wire.ConditionalStop.Previous, depth+1, archive)
		if e != nil {
			return RunnerRecord{}, e
		}
		if p.Owner != owner || p.Provider != wire.Provider {
			return RunnerRecord{}, ErrLegacyViewStale
		}
		out.ConditionalStop = &ConditionalStopRecord{Precondition: wire.ConditionalStop.Precondition, Phase: wire.ConditionalStop.Phase, Signalled: wire.ConditionalStop.Signalled, Previous: &p}
	}
	if e := validateNeutralRunnerRecord(out); e != nil {
		return RunnerRecord{}, e
	}
	if out.MaterializationCtxID != "" && !legacyContextIDPattern.MatchString(out.MaterializationCtxID) {
		return RunnerRecord{}, fmt.Errorf("invalid normalized materialization context")
	}
	if out.ProviderHostPendingFork && out.MaterializationCtxID == "" {
		return RunnerRecord{}, fmt.Errorf("pending fork requires materialization")
	}
	return out, nil
}

var legacyContextIDPattern = regexp.MustCompile(`^CTX-\d{6}-[0-9a-z]{6}$`)

func legacyNativeStateUnavailable() error {
	return fmt.Errorf("%w: legacy native goal/accounting requires its actual runtime supplier", ErrHostCapabilityUnavailable)
}
func (c *LegacyRecordCodec) decodeSnapshot(wire legacyProviderImage, archive bool) (RunnerSnapshot, error) {
	switch wire.Provider {
	case "claude":
		var s legacyClaudeSnapshot
		if e := decodeLegacyJSON(wire.Opaque, &s); e != nil {
			return RunnerSnapshot{}, e
		}
		if e := validateLegacyClaude(s); e != nil {
			return RunnerSnapshot{}, e
		}
		if !archive && len(s.OwnerRun) > 0 && !bytes.Equal(s.OwnerRun, []byte("null")) {
			return RunnerSnapshot{}, legacyNativeStateUnavailable()
		}
		out := projectClaudeSnapshot(s)
		return RunnerSnapshot{Kind: "claude", Claude: &out}, nil
	case "codex":
		var s legacyCodexSnapshot
		if e := decodeLegacyJSON(wire.Opaque, &s); e != nil {
			return RunnerSnapshot{}, e
		}
		if e := validateLegacyCodex(s); e != nil {
			return RunnerSnapshot{}, e
		}
		if !archive && (len(s.OwnerRun) > 0 && !bytes.Equal(s.OwnerRun, []byte("null")) || s.GoalTokenBudget != nil || s.PendingGoalDirective != "") {
			return RunnerSnapshot{}, legacyNativeStateUnavailable()
		}
		out, e := projectCodexSnapshotWithOwner(s)
		if e != nil {
			return RunnerSnapshot{}, e
		}
		return RunnerSnapshot{Kind: "codex", Codex: &out}, nil
	case "muse":
		var shape map[string]json.RawMessage
		if e := decodeLegacyJSON(wire.Opaque, &shape); e != nil {
			return RunnerSnapshot{}, e
		}
		n := 0
		for _, k := range []string{"profile", "profile_lock_sha256", "managed_home", "fragment_sha256"} {
			if _, ok := shape[k]; ok {
				n++
			}
		}
		if _, ok := shape["goal_transport"]; ok {
			if n != 0 {
				return RunnerSnapshot{}, fmt.Errorf("mixed Muse record variants")
			}
			var s legacyMuseSnapshot
			if e := decodeLegacyJSON(wire.Opaque, &s); e != nil {
				return RunnerSnapshot{}, e
			}
			if e := validateLegacyMuseSnapshot(&s); e != nil {
				return RunnerSnapshot{}, e
			}
			if s.Provider != "muse" || s.SessionID == "" || !filepath.IsAbs(s.WorkspaceRoot) {
				return RunnerSnapshot{}, fmt.Errorf("invalid Muse identity")
			}
			if !archive && legacyMuseHasGoalState(s) {
				text, _ := json.Marshal(s.Text)
				emptyText, _ := json.Marshal(legacyMuseTextState{})
				// Existing native shape only; the same actual runtime must
				// validate/hydrate it before any host input is exposed.
				if absentHostPort(c.museAccounting) || s.GoalTransport != "native" || !bytes.Equal(text, emptyText) {
					return RunnerSnapshot{}, legacyNativeStateUnavailable()
				}
			}
			out := projectMuseSnapshot(s)
			return RunnerSnapshot{Kind: "muse-msp", MuseMSP: &out}, nil
		}
		if n > 0 {
			if n != 4 {
				return RunnerSnapshot{}, fmt.Errorf("incomplete PTY provenance")
			}
			var s legacyPTYProvenance
			if e := decodeLegacyJSON(wire.Opaque, &s); e != nil {
				return RunnerSnapshot{}, e
			}
			if s.Profile == "" || s.ProfileLock == "" || s.FragmentSHA256 == "" || !filepath.IsAbs(s.ManagedHome) {
				return RunnerSnapshot{}, fmt.Errorf("invalid PTY provenance")
			}
			if e := validateLegacyPTY(s.legacyPTYSnapshot); e != nil {
				return RunnerSnapshot{}, e
			}
			return RunnerSnapshot{Kind: "muse-pty-provenance", MusePTYProvenance: &MusePTYProvenanceSnapshot{Base: projectPTYSnapshot(s.legacyPTYSnapshot), Profile: s.Profile, ProfileLock: s.ProfileLock, ManagedHome: s.ManagedHome, FragmentSHA256: s.FragmentSHA256}}, nil
		}
		var s legacyPTYSnapshot
		if e := decodeLegacyJSON(wire.Opaque, &s); e != nil {
			return RunnerSnapshot{}, e
		}
		if e := validateLegacyPTY(s); e != nil {
			return RunnerSnapshot{}, e
		}
		out := projectPTYSnapshot(s)
		return RunnerSnapshot{Kind: "muse-pty", MusePTY: &out}, nil
	default:
		return RunnerSnapshot{}, fmt.Errorf("unsupported normalized provider")
	}
}
func validateLegacyPTY(s legacyPTYSnapshot) error {
	if s.Contract != "task-board.muse-pty-host-v1" || s.SessionID == "" || s.PID <= 0 || !filepath.IsAbs(s.LogPath) {
		return fmt.Errorf("invalid PTY identity")
	}
	return nil
}
func legacyMuseHasGoalState(s legacyMuseSnapshot) bool {
	if s.GoalRef != nil || s.GoalObserved || s.GoalObjective != "" || s.GoalStatus != "" || s.LastAnnouncedGoal != nil || s.LastGoalLifecycle != nil || s.TokenBudget != 0 || s.TokenUsageBaseline != 0 || s.BudgetPaused || s.BudgetPauseOutcome != nil || len(s.GoalLedger) > 0 {
		return true
	}
	a, _ := json.Marshal(s.Text)
	b, _ := json.Marshal(legacyMuseTextState{})
	return !bytes.Equal(a, b)
}
func projectLegacyClaudePlan(s legacyClaudeSnapshot) LaunchSpec {
	return LaunchSpec{Claude: &ClaudePreparedLaunch{Plan: copyPrimaryPlan(s.Plan), Term: s.Term, Rows: s.Rows, Cols: s.Cols}, Executable: s.Plan.Executable, Cwd: s.Plan.ProjectDir, Variants: s.Plan.LaunchVariants, Sources: copyCodecSources(s.Plan.Sources)}
}

func (c *LegacyRecordCodec) EncodeInitial(ctx context.Context, r RunnerRecord) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	wire, e := c.mergeRecord(ctx, nil, r, 0)
	if e != nil {
		return nil, e
	}
	raw, e := json.MarshalIndent(wire, "", "  ")
	if e != nil {
		return nil, e
	}
	raw = append(raw, '\n')
	if _, e = c.DecodeLegacy(ctx, legacyRecordImage(r.SessionID, raw)); e != nil {
		return nil, e
	}
	return raw, nil
}
func (c *LegacyRecordCodec) EncodeMerge(ctx context.Context, in LegacyRecordInput, r RunnerRecord) ([]byte, error) {
	old, e := c.DecodeLegacy(ctx, in)
	if e != nil {
		return nil, e
	}
	if old.Owner != r.Owner || old.Provider != r.Provider || old.SessionID != r.SessionID {
		return nil, ErrLegacyViewStale
	}
	var before legacyRunnerRecord
	if e = decodeLegacyJSON(in.Bytes, &before); e != nil {
		return nil, e
	}
	next, e := c.mergeRecord(ctx, &before, r, 0)
	if e != nil {
		return nil, e
	}
	raw, e := encodeLegacySnapshot(in.Bytes, before, next)
	if e != nil {
		return nil, e
	}
	if _, e = c.DecodeLegacy(ctx, legacyRecordImage(r.SessionID, raw)); e != nil {
		return nil, e
	}
	return raw, nil
}

func (c *LegacyRecordCodec) mergeRecord(ctx context.Context, base *legacyRunnerRecord, r RunnerRecord, depth int) (legacyRunnerRecord, error) {
	if e := ctx.Err(); e != nil {
		return legacyRunnerRecord{}, e
	}
	if depth > 256 {
		return legacyRunnerRecord{}, fmt.Errorf("legacy recovery chain too deep")
	}
	if e := validateNeutralRunnerRecord(r); e != nil {
		return legacyRunnerRecord{}, e
	}
	owner, e := legacyOwnerForKey(r.Owner)
	if e != nil {
		return legacyRunnerRecord{}, e
	}
	var prior *legacyProviderImage
	if base != nil {
		key, e := base.Owner.key()
		if e != nil || base.SessionID != r.SessionID || key != r.Owner || base.Provider != r.Provider {
			return legacyRunnerRecord{}, ErrLegacyViewStale
		}
		owner = base.Owner
		prior = &base.ProviderHost
	}
	// A fresh nil selection cannot erase an archived explicit budget while
	// moving into live state without the actual native budget supplier.
	if e := validateCodexBudgetTransition(prior, r); e != nil {
		return legacyRunnerRecord{}, e
	}
	opaque, e := c.encodeSnapshot(r.ProviderHost, prior)
	if e != nil {
		return legacyRunnerRecord{}, e
	}
	out := legacyRunnerRecord{GracefulRetirement: r.GracefulRetirement, Contract: r.Contract, SessionID: r.SessionID, Owner: owner, Provider: r.Provider, ProjectDir: r.ProjectDir, BoardFingerprint: r.NamespaceFingerprint, State: r.State, Liveness: r.Liveness, LivenessDetail: r.LivenessDetail, OwnerRunError: r.OwnerRunError, Eviction: r.Eviction, ProviderHost: legacyProviderImage{Provider: r.Provider, Opaque: opaque}, MaterializationCtxID: r.MaterializationCtxID, ProviderHostPendingFork: r.ProviderHostPendingFork, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
	if r.AppliedGoalRef != nil {
		out.AppliedGoalRef = &legacyGoalRef{ID: r.AppliedGoalRef.ID, Revision: r.AppliedGoalRef.Revision}
	}
	if r.ConditionalStop != nil {
		var old *legacyRunnerRecord
		var original []byte
		if base != nil {
			old = &legacyRunnerRecord{}
			if base.ConditionalStop != nil {
				original = base.ConditionalStop.Previous
				if e = decodeLegacyJSON(original, old); e != nil {
					return legacyRunnerRecord{}, e
				}
			} else {
				*old = *base
				original, _ = json.Marshal(base)
			}
		}
		previous, e := c.mergeRecord(ctx, old, *r.ConditionalStop.Previous, depth+1)
		if e != nil {
			return legacyRunnerRecord{}, e
		}
		raw, e := json.Marshal(previous)
		if old != nil {
			raw, e = encodeLegacySnapshot(original, *old, previous)
		}
		if e != nil {
			return legacyRunnerRecord{}, e
		}
		out.ConditionalStop = &legacyConditionalStop{Precondition: r.ConditionalStop.Precondition, Phase: r.ConditionalStop.Phase, Signalled: r.ConditionalStop.Signalled, Previous: raw}
	}
	return out, nil
}

func (c *LegacyRecordCodec) encodeSnapshot(r ProviderHostRecord, prior *legacyProviderImage) ([]byte, error) {
	if e := validateRunnerSnapshot(r); e != nil {
		return nil, e
	}
	var original []byte
	if prior != nil {
		if prior.Provider != r.Provider {
			return nil, ErrLegacyViewStale
		}
		original = prior.Opaque
	}
	switch r.Snapshot.Kind {
	case "claude":
		var base legacyClaudeSnapshot
		if len(original) > 0 {
			if e := decodeLegacyJSON(original, &base); e != nil {
				return nil, e
			}
			if base.SessionUUID != r.Snapshot.Claude.SessionUUID {
				return nil, ErrLegacyViewStale
			}
		}
		out, e := mergeClaudeSnapshot(base, *r.Snapshot.Claude)
		if e != nil {
			return nil, e
		}
		if e = validateLegacyClaude(out); e != nil {
			return nil, e
		}
		if len(original) > 0 {
			return encodeLegacySnapshot(original, base, out)
		}
		return json.Marshal(out)
	case "codex":
		var base legacyCodexSnapshot
		if len(original) > 0 {
			if e := decodeLegacyJSON(original, &base); e != nil {
				return nil, e
			}
			if base.ThreadID != r.Snapshot.Codex.ThreadID {
				return nil, ErrLegacyViewStale
			}
		}
		out, e := mergeCodexSnapshotWithOwner(base, *r.Snapshot.Codex)
		if e != nil {
			return nil, e
		}
		if e := validateLegacyCodex(out); e != nil {
			return nil, e
		}
		if len(original) > 0 {
			return encodeLegacySnapshot(original, base, out)
		}
		return json.Marshal(out)
	case "muse-msp":
		var base legacyMuseSnapshot
		if len(original) > 0 {
			if e := decodeLegacyJSON(original, &base); e != nil {
				return nil, e
			}
			if base.SessionID != r.Snapshot.MuseMSP.SessionID {
				return nil, ErrLegacyViewStale
			}
		}
		out := mergeMuseSnapshot(base, *r.Snapshot.MuseMSP)
		if e := validateLegacyMuseSnapshot(&out); e != nil {
			return nil, e
		}
		if len(original) > 0 {
			return encodeLegacySnapshot(original, base, out)
		}
		return json.Marshal(out)
	case "muse-pty":
		out := mergePTYSnapshot(*r.Snapshot.MusePTY)
		if e := validateLegacyPTY(out); e != nil {
			return nil, e
		}
		if len(original) > 0 {
			var base legacyPTYSnapshot
			if e := decodeLegacyJSON(original, &base); e != nil {
				return nil, e
			}
			if base.SessionID != out.SessionID {
				return nil, ErrLegacyViewStale
			}
			return encodeLegacySnapshot(original, base, out)
		}
		return json.Marshal(out)
	case "muse-pty-provenance":
		v := r.Snapshot.MusePTYProvenance
		out := legacyPTYProvenance{legacyPTYSnapshot: mergePTYSnapshot(v.Base), Profile: v.Profile, ProfileLock: v.ProfileLock, ManagedHome: v.ManagedHome, FragmentSHA256: v.FragmentSHA256}
		if e := validateLegacyPTY(out.legacyPTYSnapshot); e != nil {
			return nil, e
		}
		if len(original) > 0 {
			var base legacyPTYProvenance
			if e := decodeLegacyJSON(original, &base); e != nil {
				return nil, e
			}
			if base.SessionID != out.SessionID {
				return nil, ErrLegacyViewStale
			}
			return encodeLegacySnapshot(original, base, out)
		}
		return json.Marshal(out)
	default:
		return nil, ErrLegacyCodecMismatch
	}
}

// This acknowledges only structural codec compatibility of the exact supplied
// image/hash. The host's actual persistence/readback proof is codecRecords;
// this method cannot acknowledge a private business queue or native outcome.
func (c *LegacyRecordCodec) AcknowledgeCommitted(ctx context.Context, in LegacyRecordInput) error {
	_, e := c.DecodeLegacy(ctx, in)
	return e
}

// Merge at the original JSON persistence boundary, never by serializing the
// neutral DTO as a surrogate. Retain original bytes for unchanged projections;
// preserve unrecognized members while updating only explicitly changed fields.
func encodeLegacySnapshot(original []byte, before, after any) ([]byte, error) {
	a, e := json.Marshal(before)
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(after)
	if e != nil {
		return nil, e
	}
	if bytes.Equal(a, b) && len(original) > 0 {
		return append([]byte(nil), original...), nil
	}
	if len(original) == 0 {
		return b, nil
	}
	merged, e := mergeLegacyObject(original, a, b)
	if e != nil {
		return nil, e
	}
	var pretty bytes.Buffer
	if e = json.Indent(&pretty, merged, "", "  "); e != nil {
		return nil, e
	}
	return append(pretty.Bytes(), '\n'), nil
}
func mergeLegacyObject(original, before, after []byte) ([]byte, error) {
	var raw, a, b map[string]json.RawMessage
	if e := decodeLegacyJSON(original, &raw); e != nil {
		return nil, e
	}
	if e := json.Unmarshal(before, &a); e != nil {
		return nil, e
	}
	if e := json.Unmarshal(after, &b); e != nil {
		return nil, e
	}
	for k, v := range a {
		next, exists := b[k]
		if !exists {
			delete(raw, k)
			continue
		}
		if bytes.Equal(compactLegacyJSON(v), compactLegacyJSON(next)) {
			continue
		}
		if len(v) > 0 && v[0] == '{' && len(next) > 0 && next[0] == '{' && len(raw[k]) > 0 && bytes.TrimSpace(raw[k])[0] == '{' {
			m, e := mergeLegacyObject(raw[k], v, next)
			if e != nil {
				return nil, e
			}
			raw[k] = m
		} else {
			raw[k] = next
		}
	}
	for k, v := range b {
		if _, ok := a[k]; !ok {
			raw[k] = v
		}
	}
	// Known field ordering follows the original wire declaration; unknown members
	// are retained after it. JSON output is otherwise the original writer format.
	order := []string{}
	d := json.NewDecoder(bytes.NewReader(after))
	if _, e := d.Token(); e != nil {
		return nil, e
	}
	for d.More() {
		t, e := d.Token()
		if e != nil {
			return nil, e
		}
		var discard json.RawMessage
		if e = d.Decode(&discard); e != nil {
			return nil, e
		}
		order = append(order, t.(string))
	}
	known := map[string]bool{}
	for _, k := range order {
		known[k] = true
	}
	extra := []string{}
	for k := range raw {
		if !known[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	order = append(order, extra...)
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for _, k := range order {
		v, ok := raw[k]
		if !ok {
			continue
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		key, _ := json.Marshal(k)
		out.Write(key)
		out.WriteByte(':')
		out.Write(v)
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}
