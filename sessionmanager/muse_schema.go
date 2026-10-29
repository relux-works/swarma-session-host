package sessionmanager

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

//go:embed testdata/msp-schema-1.4.1.json
var musePinnedSchemaExport []byte

// musePinnedSchemaSHA256 is the pinned 1.4.1-R4503.1 export identity. The
// byte-equality test re-exports from the pinned binary and compares.
const musePinnedSchemaSHA256 = "22e0e66232f4f67278a92d92b3636a12105776ef44d23a6bd552ebc586ef7f4d"

const musePinnedSchemaDefs = 245

// musePinnedServerVersion and musePinnedSchemaFingerprint are the initialize
// identity the host demands (r20 rule 6): the serverInfo.version the pinned
// 1.4.1-R4503.1 binary reports and the fingerprint its schema manifest
// carries. A present-but-foreign value refuses with a typed error; an absent
// value is reported explicitly, never invented. Both constants are bound by
// test to the manifest file and the real initialize capture.
const musePinnedServerVersion = "1.4.1"

const musePinnedSchemaFingerprint = "sha256:e0e163db6ccf00dbe68402ce55d6319b3edc33c421f31e9583b587b2de8a118f"

// museKnownGoodServerVersion142 and museKnownGoodSchemaFingerprint142 are
// the second qualified initialize identity (decision B): the 1.4.2-R4684.1
// release qualified by the pinned export qualification, whose schema differs from the
// 1.4.1 baseline only by additive-optional members. Optimistic mode
// accepts the pinned identity and this one silently; strict mode still
// demands the pinned pair above.
const museKnownGoodServerVersion142 = "1.4.2"

const museKnownGoodSchemaFingerprint142 = "sha256:61afea3112e0906e9dc3a536144278a74cb4b36fc6e20901a91d4432ba3568e2"

var (
	museSchemaOnce          sync.Once
	museSchemaDefs          map[string]any
	museSchemaNotifications map[string]string
	museSchemaRequests      map[string]string
	museSchemaErr           error
)

// musePinnedSchema parses the embedded export once and applies the single
// documented server-truth deviation (goalChanged goal:null, observed on the
// pinned binary; see muse_schema_deviation_test.go). It returns the $defs
// table and the notification method table.
func musePinnedSchema() (map[string]any, map[string]string, error) {
	museSchemaOnce.Do(func() {
		var exported struct {
			Defs          map[string]any `json:"$defs"`
			Notifications map[string]struct {
				Params map[string]any `json:"params"`
			} `json:"notifications"`
			Requests map[string]struct {
				Params map[string]any `json:"params"`
			} `json:"requests"`
		}
		if err := json.Unmarshal(musePinnedSchemaExport, &exported); err != nil {
			museSchemaErr = fmt.Errorf("decode pinned MSP schema export: %w", err)
			return
		}
		museSchemaDefs = exported.Defs
		table := make(map[string]string, len(exported.Notifications))
		for method, entry := range exported.Notifications {
			ref, _ := entry.Params["$ref"].(string)
			table[method] = ref
		}
		museSchemaNotifications = table
		requests := make(map[string]string, len(exported.Requests))
		for method, entry := range exported.Requests {
			ref, _ := entry.Params["$ref"].(string)
			requests[method] = ref
		}
		museSchemaRequests = requests
		museSchemaErr = museApplySchemaDeviations(museSchemaDefs)
	})
	return museSchemaDefs, museSchemaNotifications, museSchemaErr
}

// musePinnedServerRequests returns the export's server-initiated request
// method table (approval/request, userInput/request) for params validation.
func musePinnedServerRequests() (map[string]any, map[string]string, error) {
	defs, _, err := musePinnedSchema()
	if err != nil {
		return nil, nil, err
	}
	return defs, museSchemaRequests, nil
}

// museApplySchemaDeviations aligns the export with observed pinned-binary
// wire truth where the export contradicts the server. Every deviation is
// narrow, pinned by a real-capture test, and recorded here:
//
//   - C1 SessionGoalChangedParams.goal is a plain Goal $ref in the export,
//     but the pinned binary emits "goal": null on goal/clear (captured
//     2026-10-02, testdata/msp-real-captures-1.4.1.json). The sibling
//     SnapshotState.goal node is already anyOf [Goal, null], and the
//     params description itself says "or null to clear". The effective
//     schema accepts null here; any other shape still validates as Goal.
//   - C2 SessionTokenUsageParams.modelId is a plain string in the export,
//     but the pinned binary emits "modelId": null on echo sessions started
//     without a model (captured 2026-10-02, tokenUsageNullModel in
//     testdata/msp-real-captures-1.4.1.json; the node description itself
//     documents "`null` on pre-schema records"). The effective schema
//     accepts null here; any other non-string still fails.
//   - C3 SnapshotState.name is a plain optional string in the export, but
//     the pinned binary emits "name": null on session/resume snapshots
//     (captured 2026-10-02, session/resume entries in
//     testdata/msp-real-results-1.4.1.json; the node description itself
//     documents "`null` when the session ..."). The effective schema
//     accepts null here; any other non-string still fails.
//
// Each rule is its own function so a narrowing mutant can target exactly
// one admission without touching the others.
func museApplySchemaDeviations(defs map[string]any) error {
	if err := museDeviateGoalChangedGoalNull(defs); err != nil {
		return err
	}
	if err := museDeviateTokenUsageModelIDNull(defs); err != nil {
		return err
	}
	if err := museDeviateSnapshotStateNameNull(defs); err != nil {
		return err
	}
	return nil
}

func museDeviateGoalChangedGoalNull(defs map[string]any) error {
	params, ok := defs["SessionGoalChangedParams"].(map[string]any)
	if !ok {
		return fmt.Errorf("pinned schema lacks SessionGoalChangedParams")
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		return fmt.Errorf("SessionGoalChangedParams has no properties")
	}
	goal, ok := props["goal"].(map[string]any)
	if !ok {
		return fmt.Errorf("SessionGoalChangedParams has no goal property")
	}
	if _, hasAnyOf := goal["anyOf"]; hasAnyOf {
		return nil
	}
	ref, _ := goal["$ref"].(string)
	if ref != "#/$defs/Goal" {
		return fmt.Errorf("SessionGoalChangedParams.goal is not the known Goal $ref: %q", ref)
	}
	description, _ := goal["description"].(string)
	props["goal"] = map[string]any{
		"anyOf": []any{
			map[string]any{"$ref": "#/$defs/Goal"},
			map[string]any{"type": "null"},
		},
		"description": description,
	}
	return nil
}

func museDeviateTokenUsageModelIDNull(defs map[string]any) error {
	params, ok := defs["SessionTokenUsageParams"].(map[string]any)
	if !ok {
		return fmt.Errorf("pinned schema lacks SessionTokenUsageParams")
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		return fmt.Errorf("SessionTokenUsageParams has no properties")
	}
	modelID, ok := props["modelId"].(map[string]any)
	if !ok {
		return fmt.Errorf("SessionTokenUsageParams has no modelId property")
	}
	if _, hasAnyOf := modelID["anyOf"]; hasAnyOf {
		return nil
	}
	typ, _ := modelID["type"].(string)
	if typ != "string" {
		return fmt.Errorf("SessionTokenUsageParams.modelId is not the known string node: %q", typ)
	}
	description, _ := modelID["description"].(string)
	props["modelId"] = map[string]any{
		"anyOf": []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "null"},
		},
		"description": description,
	}
	return nil
}

func museDeviateSnapshotStateNameNull(defs map[string]any) error {
	state, ok := defs["SnapshotState"].(map[string]any)
	if !ok {
		return fmt.Errorf("pinned schema lacks SnapshotState")
	}
	props, ok := state["properties"].(map[string]any)
	if !ok {
		return fmt.Errorf("SnapshotState has no properties")
	}
	name, ok := props["name"].(map[string]any)
	if !ok {
		return fmt.Errorf("SnapshotState has no name property")
	}
	if _, hasAnyOf := name["anyOf"]; hasAnyOf {
		return nil
	}
	typ, _ := name["type"].(string)
	if typ != "string" {
		return fmt.Errorf("SnapshotState.name is not the known string node: %q", typ)
	}
	description, _ := name["description"].(string)
	props["name"] = map[string]any{
		"anyOf": []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "null"},
		},
		"description": description,
	}
	return nil
}

// museStrictClientMethods are the notification families the MSP client types
// for the Muse host. Every one must resolve to a params definition in the
// pinned export; unknown families pass through as raw frames.
var museStrictClientMethods = []string{
	"session/goalChanged",
	"session/statusChanged",
	"session/tokenUsage",
	"turn/started",
	"turn/completed",
	"turn/unqueued",
}

// museStrictClientParamsDef resolves the params definition for a client-typed
// notification method. It reports false for families the client leaves raw.
func museStrictClientParamsDef(method string) (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	for _, known := range museStrictClientMethods {
		if known == method {
			ref := table[method]
			def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
			return def, ok && def != nil
		}
	}
	return nil, false
}

// museStrictItemParamsDef resolves the item envelope definition for an item
// notification family.
func museStrictItemParamsDef(method string) (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	switch method {
	case "item/started", "item/updated", "item/completed":
	default:
		return nil, false
	}
	ref := table[method]
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}

// museStrictSessionClosedParamsDef resolves the session/closed params
// definition: the unload notice the owner wait ends on (r21 C4).
func museStrictSessionClosedParamsDef() (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	ref := table["session/closed"]
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}

// museStrictViewHealthChangedParamsDef resolves the session/viewHealthChanged
// params definition: the stream-loss notice that taints the turn (r21 C1).
func museStrictViewHealthChangedParamsDef() (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	ref := table["session/viewHealthChanged"]
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}

// museStrictViewGapParamsDef resolves the view/gap params definition.
func museStrictViewGapParamsDef() (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	ref := table["view/gap"]
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}

// museStrictNotificationParamsDef resolves the params definition for ANY
// notification method in the pinned export's notifications table. The
// closed dispatch table (muse_dispatch.go) validates every raw-routed
// method through this single resolver, so validate-before-attribute holds
// by construction for each tabled family. It reports false for methods the
// export does not know (client-to-server "initialized" has no params node)
// and for methods whose params carry no $ref.
func museStrictNotificationParamsDef(method string) (map[string]any, bool) {
	defs, table, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	ref, ok := table[method]
	if !ok || strings.TrimSpace(ref) == "" {
		return nil, false
	}
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}

// musePinnedNotificationMethods returns the export's server-to-host
// notification method set: every key of the notifications table except the
// client-to-server "initialized" handshake, which carries no params and
// never arrives on the host's read path. The dispatch drift test pins the
// table against exactly this set.
func musePinnedNotificationMethods() ([]string, error) {
	_, table, err := musePinnedSchema()
	if err != nil {
		return nil, err
	}
	methods := make([]string, 0, len(table))
	for method := range table {
		if method == "initialized" {
			continue
		}
		methods = append(methods, method)
	}
	return methods, nil
}

// musePinnedServerRequestMethods returns the export's server-initiated
// request method set.
func musePinnedServerRequestMethods() ([]string, error) {
	_, table, err := musePinnedServerRequests()
	if err != nil {
		return nil, err
	}
	methods := make([]string, 0, len(table))
	for method := range table {
		methods = append(methods, method)
	}
	return methods, nil
}

// musePinnedRPCMethods returns the export's client-to-server RPC method set
// (the methods table's keys). The dispatch drift test pins every rpc-result
// table row against this set.
func musePinnedRPCMethods() ([]string, error) {
	var exported struct {
		Methods map[string]any `json:"methods"`
	}
	if err := json.Unmarshal(musePinnedSchemaExport, &exported); err != nil {
		return nil, fmt.Errorf("decode pinned MSP schema methods: %w", err)
	}
	methods := make([]string, 0, len(exported.Methods))
	for method := range exported.Methods {
		methods = append(methods, method)
	}
	return methods, nil
}

// musePinnedSchemaKnowsNotification reports whether the export's
// notifications table carries a method, including the client-to-server
// "initialized" handshake.
func musePinnedSchemaKnowsNotification(method string) bool {
	_, table, err := musePinnedSchema()
	if err != nil {
		return false
	}
	_, ok := table[method]
	return ok
}

// museStrictServerRequestParamsDef resolves the params definition for a
// server-initiated request method (approval/request, userInput/request). It
// reports false for methods the export's requests table does not know.
func museStrictServerRequestParamsDef(method string) (map[string]any, bool) {
	defs, table, err := musePinnedServerRequests()
	if err != nil {
		return nil, false
	}
	switch method {
	case "approval/request", "userInput/request":
	default:
		return nil, false
	}
	ref := table[method]
	def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def, ok && def != nil
}
