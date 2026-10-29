// Curator-first composition input for the task-board muse launcher (R3,
// regression).
//
// The launcher resolves its managed environment through direct calls,
// `curator env resolve muse --format json` first and one gated --repair
// only for a stale or unprovisioned home, per the composition ruling
// recorded on the task: failure refuses (never a bare launch, never
// agents-infra), and the session record keeps the profile, the profile lock,
// the managed home and the fragment sha256. The command in cmd/muse_manager.go
// owns the curator process invocation.
//
// The fragment schema is the module's, not a second spelling kept here.
// DecodeMuseCuratorFragment decodes into agentic.CuratorContext with unknown
// members refused, then validates through agentic.ValidateCuratorContext —
// the same two calls the spawn path makes in decodeChildContext — so a
// member the module contract accepts (such as the v3 permissions object,
// regression) can never be refused here for being unknown. The spawn
// path additionally proves its bytes canonical CCJ-1 before the typed
// decode; that check does not apply to the bytes this decoder reads, which
// are wire bytes (MuseFragmentWireBytes compacts and HTML-escapes them while
// preserving key order), not raw curator stdout.
//
// Managed Muse requires launch-env-fragment-v4: four XDG homes, the file
// credential backend and credential_store policy, plus optional root_context.
// HOME remains ambient. The module validates policy and home shape; this wrapper
// preserves its typed version, profile and descriptor refusal codes.
package musepty

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"path/filepath"
	"strings"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// MuseFragmentV4 is the managed Muse revision required by curator-spec §10.2.
// MuseFragmentEnvironment is the only fragment environment it accepts.
const (
	MuseFragmentV4          = agentic.CuratorLaunchFragmentV4
	MuseFragmentEnvironment = "muse"
)

// The four XDG parents a v4 fragment carries. XDG_RUNTIME_DIR is not
// among them: it is a login-session path, not a curator-provisioned
// managed location, and it keeps arriving through the parent environment.
const (
	MuseXDGDataHome   = "XDG_DATA_HOME"
	MuseXDGConfigHome = "XDG_CONFIG_HOME"
	MuseXDGCacheHome  = "XDG_CACHE_HOME"
	MuseXDGStateHome  = "XDG_STATE_HOME"
)

var museFragmentEnvNames = []string{
	MuseXDGDataHome,
	MuseXDGConfigHome,
	MuseXDGCacheHome,
	MuseXDGStateHome,
	agentic.CuratorMuseCredentialBackendVariable,
}

// MuseFragmentError is a refusing composition result, never a degraded
// launch. Callers match the Code; the cause carries the operator-safe
// detail (field names and the offending member, never curator stderr,
// which can contain private paths).
type MuseFragmentError struct {
	Code  string
	cause error
}

func (e *MuseFragmentError) Error() string {
	return "muse composition refused [" + e.Code + "]: " + e.cause.Error()
}
func (e *MuseFragmentError) Unwrap() error { return e.cause }

const (
	// MuseFragmentToolUnavailable refuses a missing curator binary.
	MuseFragmentToolUnavailable = "muse_curator_unavailable"
	// MuseFragmentResolveFailed refuses a nonzero curator exit that names
	// no missing profile.
	MuseFragmentResolveFailed = "muse_curator_resolution_failed"
	// MuseFragmentProfileMissing refuses a fragment without its profile
	// pin, or a curator run that reports the profile unknown.
	MuseFragmentProfileMissing = "muse_fragment_profile_missing"
	// MuseFragmentMalformed refuses a fragment that is not well-formed
	// v4: bad JSON, a wrong environment, an invalid pin or precedence, a
	// missing or non-absolute XDG parent, or HOME set.
	MuseFragmentMalformed = "muse_fragment_malformed"
	// MuseFragmentVersionUnknown refuses any fragment revision but v4.
	MuseFragmentVersionUnknown = "muse_fragment_version_unknown"
	// MuseFragmentDescriptorUnknown refuses an env member outside the
	// XDG homes and the credential backend.
	MuseFragmentDescriptorUnknown = "muse_fragment_descriptor_unknown"
	// MuseFragmentPermissionsLocked refuses a posture wider than the
	// curator fragment's locked permission mode. The spawn path's
	// headless-child gate reuses this same constant: one code, one
	// spelling, both twins.
	MuseFragmentPermissionsLocked = "muse_fragment_permissions_locked"
	// MuseFragmentRepairRequiresOperator refuses an automatic --repair
	// under a curator release outside the 0.14 line (R241 P12(c)): newer
	// releases migrate the whole profile under repair, so the launcher
	// never invokes it there and the operator repairs out of band. The
	// code string is shared with the tracked-child resolver's gate.
	MuseFragmentRepairRequiresOperator = "curator_repair_requires_operator"
)

func museFragmentRefused(code string, cause error) error {
	return NewMuseFragmentError(code, cause)
}

// NewMuseFragmentError constructs the typed refusal for curator-side
// failures the wrapper detects itself (missing binary, failed resolve).
func NewMuseFragmentError(code string, cause error) error {
	return &MuseFragmentError{Code: code, cause: cause}
}

// MuseCuratorFragment is the typed v4 projection: the module's own
// CuratorContext, not a second hand-kept schema. Env carries the credential backend and
// four XDG parents; HOME is never a member.
type MuseCuratorFragment = agentic.CuratorContext

// DecodeMuseCuratorFragment strictly decodes and validates one v4 document
// through the module contract: unknown members are refused against the
// module's own CuratorContext, then the typed value is validated by
// agentic.ValidateCuratorContext. The Muse-only checks stay layered around
// the module call: the v4 revision and muse environment pins run ahead of
// it (the module admits other revisions and environments this launcher
// never runs), and the env member rule runs ahead of it (the module reports
// a foreign or missing XDG member as malformed, while this path keeps the
// descriptor-unknown code and the per-member messages the wrapper surface
// pins). The profile pin, the precedence, the XDG value shape and the
// permissions member are the module's rules now; the local spellings are
// gone. Every refusal class is typed: malformed shape, unknown version,
// missing profile pin, or unknown descriptor.
func DecodeMuseCuratorFragment(document []byte) (*MuseCuratorFragment, error) {
	var fragment agentic.CuratorContext
	if err := decodeMuseStrictJSON(document, &fragment); err != nil {
		return nil, museFragmentRefused(MuseFragmentMalformed, err)
	}
	// Let the module supply its typed v3 refusal and operator upgrade route,
	// before MuseHomes (which deliberately accepts v4 only) can mask it.
	if fragment.Revision == agentic.CuratorLaunchFragmentV3 {
		if err := agentic.ValidateCuratorContext(&fragment, ""); err != nil {
			return nil, classifyMuseModuleError(err)
		}
	}
	if fragment.Revision != MuseFragmentV4 {
		return nil, museFragmentRefused(MuseFragmentVersionUnknown,
			fmt.Errorf("fragment revision %q is not %q", fragment.Revision, MuseFragmentV4))
	}
	if fragment.Environment != MuseFragmentEnvironment {
		return nil, museFragmentRefused(MuseFragmentMalformed,
			fmt.Errorf("fragment environment %q is not %q", fragment.Environment, MuseFragmentEnvironment))
	}
	for name := range fragment.Env {
		switch name {
		case MuseXDGDataHome, MuseXDGConfigHome, MuseXDGCacheHome, MuseXDGStateHome, agentic.CuratorMuseCredentialBackendVariable:
		case "HOME":
			return nil, museFragmentRefused(MuseFragmentMalformed,
				errors.New("fragment must never set HOME"))
		default:
			return nil, museFragmentRefused(MuseFragmentDescriptorUnknown,
				fmt.Errorf("unknown fragment descriptor %q", name))
		}
	}
	for _, want := range museFragmentEnvNames {
		if _, ok := fragment.Env[want]; !ok {
			return nil, museFragmentRefused(MuseFragmentMalformed,
				fmt.Errorf("fragment is missing %q", want))
		}
	}
	// The module derives the managed home for muse itself: the shared XDG
	// parent its own shape check proves. The parent below is what the
	// validator binds the request to, never a second spelling.
	homes, err := fragment.MuseHomes()
	if err != nil {
		return nil, classifyMuseModuleError(err)
	}
	if err := agentic.ValidateCuratorContext(&fragment, homes.Parent); err != nil {
		return nil, classifyMuseModuleError(err)
	}
	return &fragment, nil
}

// classifyMuseModuleError routes a module validation refusal onto the
// wrapper's typed codes. The module owns the revision, profile, precedence
// and permissions rules; this path keeps its own codes for the classes the
// wrapper surface pins. An unknown revision cannot reach this line — the v4
// pin above refuses it first — so every other module refusal is malformed,
// exactly like the spawn path's classifier.
func classifyMuseModuleError(err error) error {
	switch {
	case errors.Is(err, agentic.ErrCuratorFragmentRevisionUnsupported):
		return museFragmentRefused(MuseFragmentVersionUnknown, err)
	case errors.Is(err, agentic.ErrCuratorProfileMissing):
		return museFragmentRefused(MuseFragmentProfileMissing, err)
	case errors.Is(err, agentic.ErrUnknownCuratorDescriptor):
		return museFragmentRefused(MuseFragmentDescriptorUnknown, err)
	default:
		return museFragmentRefused(MuseFragmentMalformed, err)
	}
}

func decodeMuseStrictJSON(document []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(document)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}

// MuseFragmentWireBytes renders one curator fragment document in the exact
// representation the launch envelope carries: compact JSON with the
// envelope's HTML escaping applied. The wrapper digests these bytes, and
// the envelope marshal is a fixed point over them, so the daemon hashes
// byte-identical content instead of re-encoded bytes. Empty and invalid
// documents refuse here; the caller maps them to the malformed refusal.
func MuseFragmentWireBytes(document []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(document)
	if len(trimmed) == 0 {
		return nil, errors.New("curator fragment is empty")
	}
	return json.Marshal(json.RawMessage(trimmed))
}

// MuseFragmentDigest binds the exact transmitted fragment bytes. Both the
// launcher and the daemon digest the same bytes, so a mismatch is tamper
// or corruption, never an encoding disagreement.
func MuseFragmentDigest(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// MuseFragmentManagedHome is the managed home the session record keeps:
// the fragment's XDG_DATA_HOME, where the Muse session log lives.
func MuseFragmentManagedHome(fragment *MuseCuratorFragment) string {
	if fragment == nil {
		return ""
	}
	return fragment.Env[MuseXDGDataHome]
}

// MergeMuseFragmentEnv overlays the fragment's four XDG homes and backend onto the
// parent environment. The credential backend is also module-owned. HOME and
// every other entry pass through byte-identical and are never replaced.
func MergeMuseFragmentEnv(parent []string, fragment *MuseCuratorFragment) []string {
	if fragment == nil {
		return append([]string(nil), parent...)
	}
	merged := append([]string(nil), parent...)
	for _, name := range museFragmentEnvNames {
		merged = agentic.SetEnvValue(merged, name, fragment.Env[name])
	}
	return merged
}

// MuseBinarySelection names the muse binary a launch will run. Empty
// selects PATH resolution through the launch environment. It is the ONE
// seam where regression (frozen newest build) will plug its
// frozen path in later; nothing else in the launcher resolves the binary.
type MuseBinarySelection = sm.MuseBinarySelection

// ApplyMuseBinarySelection honours a binary selection by floating its
// directory to the front of PATH, so the module's PATH resolution lands
// on exactly that build. Empty leaves the environment untouched. A set
// selection must be absolute; the module's probe refuses an unrunnable
// one, so this layer checks the shape and nothing else.
func ApplyMuseBinarySelection(env []string, selection MuseBinarySelection) ([]string, error) {
	if selection == "" {
		return env, nil
	}
	path := strings.TrimSpace(string(selection))
	if path == "/" || !strings.HasPrefix(path, "/") || strings.IndexByte(path, 0) >= 0 {
		return nil, fmt.Errorf("muse binary selection must be an absolute file path")
	}
	dir := filepath.Dir(path)
	if existing := envValue(env, "PATH"); existing != "" {
		return agentic.SetEnvValue(env, "PATH", dir+":"+existing), nil
	}
	return agentic.SetEnvValue(env, "PATH", dir), nil
}

// MuseTrustWorkspaceArg is the workspace-trust argument the launcher
// composes into every interactive plan. R2 refuses plans without it, and
// there is no other supplier: the operator must not have to spell a
// requirement the host mandates.
const MuseTrustWorkspaceArg = "--trust-workspace"

const museResumeSelector = "resume"

// ValidateMuseOperatorNativeArgs refuses the one operator token the
// manager owns: resume selection. A cold start carrying `resume <ref>`
// would attach the manager to a native session it never recorded, with
// no adoption ceremony and no conflict check, so the wrapper fails fast
// here and the factory re-checks the same boundary authoritatively.
// Resume selection is manager-composed from the durable record only.
func ValidateMuseOperatorNativeArgs(args []string) error {
	for _, arg := range args {
		if arg == museResumeSelector {
			return fmt.Errorf("native argument %q is manager-composed: resume selection comes from the recorded session, never from operator argv (use: task-board muse resume SES-HANDLE)", arg)
		}
	}
	return nil
}

// MuseLaunchRequestParams is the complete input to one interactive Muse
// launch request. The launcher builds it from the curator fragment and
// the wrapper flags; the daemon rebuilds it byte-identically from the
// launch payload, so both sides validate the same request.
type MuseLaunchRequestParams struct {
	Model           string
	Effort          string
	EffortRequired  bool
	WorkDir         string
	Env             []string
	Fragment        *MuseCuratorFragment
	Yolo            bool
	ToolRelease     string
	OperatorArgs    []string
	BinarySelection MuseBinarySelection
	// ResumeSessionID is factory-set from the durable record on the
	// resume path only. It never arrives from the operator.
	ResumeSessionID string
}

// BuildMuseLaunchRequest composes the module launch request: fragment env
// merged over the parent env (HOME untouched and required present),
// binary selection applied, posture mapped (native by default, yolo only
// when asked), and trust plus any manager-composed resume selection ahead
// of the verbatim operator suffix.
func BuildMuseLaunchRequest(params MuseLaunchRequestParams) (agentic.LaunchRequest, error) {
	if params.Fragment == nil {
		return agentic.LaunchRequest{}, fmt.Errorf("muse launch requires a resolved curator fragment")
	}
	if err := ValidateMuseOperatorNativeArgs(params.OperatorArgs); err != nil {
		return agentic.LaunchRequest{}, err
	}
	if strings.TrimSpace(params.Model) == "" {
		return agentic.LaunchRequest{}, fmt.Errorf("muse launch requires a model")
	}
	if strings.TrimSpace(params.WorkDir) == "" {
		return agentic.LaunchRequest{}, fmt.Errorf("muse launch requires a working directory")
	}
	merged := MergeMuseFragmentEnv(params.Env, params.Fragment)
	if envValue(merged, "HOME") == "" {
		return agentic.LaunchRequest{}, fmt.Errorf("muse launch environment is missing HOME: HOME is never replaced and must arrive intact")
	}
	selected, err := ApplyMuseBinarySelection(merged, params.BinarySelection)
	if err != nil {
		return agentic.LaunchRequest{}, err
	}
	mode := agentic.PermissionModeNative
	if params.Yolo {
		mode = agentic.PermissionModeYolo
	}
	// Validated locked fragments are native; yolo is the only wider
	// posture. Refuse here, before either caller probes or plans.
	if permissions := params.Fragment.Permissions; permissions != nil && permissions.Locked && mode == agentic.PermissionModeYolo {
		return agentic.LaunchRequest{}, museFragmentRefused(MuseFragmentPermissionsLocked,
			fmt.Errorf("permissions locked by source %q to mode %q: --yolo is not allowed", permissions.Source, permissions.Mode))
	}
	var native []string
	if id := strings.TrimSpace(params.ResumeSessionID); id != "" {
		native = append(native, museResumeSelector, id)
	}
	native = append(native, MuseTrustWorkspaceArg)
	native = append(native, params.OperatorArgs...)
	model := agentic.Model{ID: strings.TrimSpace(params.Model)}
	if params.EffortRequired {
		model.Effort = agentic.EffortSupportRequired
	}
	homes, err := params.Fragment.MuseHomes()
	if err != nil {
		return agentic.LaunchRequest{}, classifyMuseModuleError(err)
	}
	return agentic.LaunchRequest{
		System:         "muse",
		Model:          model,
		Effort:         strings.TrimSpace(params.Effort),
		WorkDir:        params.WorkDir,
		Home:           homes.Parent,
		Context:        params.Fragment,
		Env:            selected,
		PermissionMode: mode,
		ToolRelease:    strings.TrimSpace(params.ToolRelease),
		NativeArgs:     native,
	}, nil
}

// ProbeMuseToolRelease establishes the running tool's release triple by
// probing the resolved binary, through the plugin's own prober. The
// launcher calls it for every interactive posture. An undetected answer
// refuses yolo; native continues with no caller release assertion.
func ProbeMuseToolRelease(ctx context.Context, env []string) (string, error) {
	release, err := muse.New().ProbeToolRelease(ctx, env)
	if err != nil {
		return "", err
	}
	return release, nil
}
