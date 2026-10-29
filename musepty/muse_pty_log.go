package musepty

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

type museRunTerminal struct {
	RunID    string
	Terminal string
	Text     string
}

// Per-key lifecycle states. Absence from the map is the absent state; a
// terminated key stays terminated for the rest of the fold.
type museRunState int

const (
	museRunActive museRunState = 1 + iota
	museRunTerminated
)

type museTaskState int

const (
	museTaskProposed museTaskState = 1 + iota
	museTaskAccepted
	museTaskScheduled
	museTaskIntent
	museTaskStarted
	museTaskTerminated
)

// Session epoch states (r13). The owned session moves None -> Open on
// its first opened record; Open -> Ended on session.end; Ended ->
// ReopenPending on a reopen opened; Open -> ReopenPending on a reopen
// opened without an end; ReopenPending -> Open on a resolved
// session.resumed. Any other edge is unknown state (the opened, end
// and resumed arms below refuse it). Runtime records are admitted
// only in an OPEN epoch: work recorded after an end and before a
// resolved reopen is unknown state, never laundered by the reopen.
// Route facts keep their own process-binding rules in every epoch
// (the captured resume order is end, route, opened, resumed), and
// session-informational records without fold state keep the default
// arm. The three bools on the fold stay the machine's representation;
// this enum names the states the admission rule reads.
type museEpochState int

const (
	museEpochNone museEpochState = iota
	museEpochOpen
	museEpochEnded
	museEpochReopenPending
)

func (f *museLogFold) epoch() museEpochState {
	switch {
	case !f.Opened:
		return museEpochNone
	case f.ReopenPending:
		return museEpochReopenPending
	case f.Ended:
		return museEpochEnded
	default:
		return museEpochOpen
	}
}

// Every runtime record is admitted only in an OPEN epoch of the owned
// session (r13, J49). The id is the record's primary joined identity
// (run, task, intent, command, or "" for the presence-only agent tree
// record); it names the refused member and keys the narrowing mutant.
func museRequireOpenEpoch(f *museLogFold, kind, id string) error {
	if f.epoch() != museEpochOpen {
		if id == "" {
			return fmt.Errorf("Muse %s record outside an open session epoch", kind)
		}
		return fmt.Errorf("Muse %s %q outside an open session epoch", kind, id)
	}
	return nil
}

// The captured task order, run-bound and mirror alike: proposed, accepted,
// scheduled, side_effect_intent, started, then exactly one terminal. Any
// edge outside this table is unknown state.
var museTaskTransitions = map[museTaskState]map[string]museTaskState{
	0: {
		"proposed": museTaskProposed,
	},
	museTaskProposed: {
		"accepted": museTaskAccepted,
	},
	museTaskAccepted: {
		"scheduled": museTaskScheduled,
	},
	museTaskScheduled: {
		"side_effect_intent": museTaskIntent,
	},
	museTaskIntent: {
		"started": museTaskStarted,
	},
	museTaskStarted: {
		"completed": museTaskTerminated,
		"failed":    museTaskTerminated,
		"cancelled": museTaskTerminated,
	},
}

// Auxiliary run events observed in the captures. Each is admitted only
// inside an active run; anything else, even inside an active run, is
// unknown state. Six bind identity in dedicated arms above
// (model_response_created, reasoning_committed,
// model_request_configured, model_input_trace_recorded,
// context_block_diagnostic, memory_reminder_child_session_linked); the
// rest are self-loops that leave state unchanged.
var museRunAuxEvents = map[string]bool{
	"context_block_diagnostic":             true,
	"task_stream_linked":                   true,
	"memory_reminder_child_session_linked": true,
	"model_request_configured":             true,
	"provider_request_options_configured":  true,
	"model_input_trace_recorded":           true,
	"model_response_created":               true,
	"goal_usage_attribution":               true,
	"model_completed":                      true,
	"reasoning_committed":                  true,
	"resource_usage_sampled":               true,
	"reasoning_summary_delta":              true,
	"reasoning_summary_committed":          true,
	"assistant_tool_calls_committed":       true,
	"tool_result_batch_committed":          true,
	"context_block_updated":                true,
	"reminder_proposal":                    true,
	"skill_reminder_decision":              true,
	"reminder_reconciler_outcome":          true,
}

// A queued inbox item keeps its full identity and content. The causation of
// the captured queue records names the run the item later drains into.
type museQueuedItem struct {
	Run       string
	Causation string
	Content   []byte
}

// A cleanup effect is keyed by the run that owns it and the effect id the
// TUI assigned: a terminal discharges only the started effect with its
// exact key. Absence from the map is the absent state; a terminated key
// stays terminated for the rest of the fold.
type museCleanupKey struct {
	Run    string
	Effect int
}

type museCleanupState int

const (
	museCleanupStarted museCleanupState = 1 + iota
	museCleanupTerminated
)

// An accepted user intent stays pending until its materialized outcome
// references the stored intent. The accepted envelope identity (outer
// record id and sequence) is stored: the materialized outcome must name
// that exact envelope, and a top-level outcome must name the run the
// intent produced. Absence from the map is the absent state; a
// materialized intent stays materialized for the rest of the fold.
type museIntentState int

const (
	museIntentAccepted museIntentState = 1 + iota
	museIntentMaterialized
)

// The stored accepted intent: its lifecycle state, the envelope
// identity its materialized outcome must reference, and the fold index
// that orders the acceptance before the run it produced.
type museIntent struct {
	State       museIntentState
	RecordID    string
	Sequence    int
	AcceptIndex int
}

// A started run keeps the record identity an intent outcome must name:
// the outer record id and the source run record id, plus the fold index
// that orders the start after the acceptance that produced it.
type museStartedRecord struct {
	ID       string
	SourceID string
	Index    int
}

// A queued inbox item observed under its outer record id. The drain
// discharges the item; the delivery reference resolves against this
// observation whatever the drain state. The observation keeps the
// queue's forward causation, its owning run, its source item and its
// fold index: an active-run delivery must cite the queue of its own
// intent and run, observed after the acceptance it drains for, and a
// later queue reusing the envelope for another source item is unknown
// state. An observation is never overwritten.
type museObservedQueue struct {
	Causation string
	Run       string
	SourceID  string
	Index     int
}

// An auxiliary identity a run event cites and the fold resolves after
// the fold against the run's configured identities: a trace names a
// configured request source, a diagnostic names a configured context
// block, and a reminder link names a configured roster member.
type museAuxRef struct {
	Run string
	ID  string
}

// A materialized outcome the fold has not resolved yet. A top-level
// turn resolves to the started record of the run the intent produced;
// an active-run delivery resolves to an observed queued record.
type musePendingOutcome struct {
	Intent            string
	Kind              string
	Run               string
	StartedSessionRec string
	StartedSourceRec  string
	DeliveryRec       string
}

// A committed message is keyed by the run that committed it and the
// provider message id: the same message twice in one run is a replay,
// never a second commit.
type museMessageKey struct {
	Run string
	ID  string
}

type museToolEffectKey struct {
	Run, Effect string
}

type museToolEffect struct {
	Task, Call string
	ModelIndex int
	Terminal   bool
}

type museLogFold struct {
	Opened, Ended, ReopenPending bool
	Unknown                      bool
	// StartupPrefixWitnessed reports that the log's first three complete
	// records all validated under the structural startup witness (L3G).
	// The host admits startup only on this flag plus an opened session;
	// fragment folding never sets it.
	StartupPrefixWitnessed bool
	PID                    int
	pidRebind              bool
	SessionID              string
	RunLifecycle           map[string]museRunState
	Queued                 map[string]museQueuedItem
	QueueDrained           map[string]bool
	DrainTargetPending     map[string]bool
	Tasks                  map[museTaskKey]museTaskState
	ToolEffects            map[museToolEffectKey]museToolEffect
	TaskTerminals          map[museTaskKey]string
	TaskTerminalIndex      map[museTaskKey]int
	TaskIdempotency        map[museTaskKey]string
	Mirror                 map[string]museTaskState
	MirrorIdempotency      map[string]string
	Cleanup                map[museCleanupKey]museCleanupState
	CleanupChild           map[museCleanupKey]string
	CleanupStartIndex      map[museCleanupKey]int
	Intents                map[string]museIntent
	Started                []string
	StartedRecords         map[string]museStartedRecord
	ObservedQueue          map[string]museObservedQueue
	PendingOutcomes        []musePendingOutcome
	Responses              map[string]map[string]bool
	MessageResponses       map[string]map[string]bool
	MessageIDs             map[museMessageKey]bool
	Terminals              map[string]museRunTerminal
	Messages               map[string]string
	IntakeReceived         map[string]bool
	IntakeSettled          map[string]bool
	// SessionStartCommand is the latest 1.4.10 session-start command id,
	// received before the first open or after an end. It is pending until
	// the next opened record; SessionStartSeen records its intake_v3 and
	// started records, each at most once.
	SessionStartCommand string
	SessionStartPending bool
	SessionStartSeen    map[string]bool
	ConfiguredSources   map[string]map[string]bool
	ConfiguredContexts  map[string]map[string]bool
	ConfiguredRoster    map[string]map[string]bool
	TraceRefs           []museAuxRef
	DiagRefs            []museAuxRef
	ReminderRefs        []museAuxRef
	// Global envelope uniqueness (r12): one set of every outer
	// record envelope id and every sequence observed in the owned
	// stream. Any repeat, anywhere, is unknown state. RecordIndex
	// doubles as the seen-id set; it also resolves resume
	// references to their recorded record.
	SeenSeqs    map[int]bool
	EpochOpens  []int
	RecordIndex map[string]int
	RecordSeq   map[string]int
}

// Pending task work is keyed by the (run, task) pair the TUI started. A task
// terminal discharges only the pending pair with its exact key; a terminal
// under any other run is unknown state, never a discharge.
type museTaskKey struct {
	Run  string
	Task string
}

func readMuseLog(path string) (*museLogFold, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// A partial record is unknown, not evidence of no work.
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return nil, fmt.Errorf("Muse log has no complete tail")
	}
	// L3G startup-prefix witness first: a structurally unsound prefix
	// refuses typed here, ahead of the full fold. An incomplete window
	// is not a refusal: fragments and mid-startup logs proceed to the
	// legacy fold exactly as before, but without the witnessed flag the
	// host needs for startup admission.
	witnessed := true
	if err := ValidateMuseStartupPrefix(data); err != nil {
		var prefix *MuseStartupPrefixError
		if !errors.As(err, &prefix) || prefix.Code != MuseStartupPrefixIncomplete {
			return nil, err
		}
		witnessed = false
	}
	f := &museLogFold{RunLifecycle: map[string]museRunState{}, Queued: map[string]museQueuedItem{}, QueueDrained: map[string]bool{}, DrainTargetPending: map[string]bool{}, Tasks: map[museTaskKey]museTaskState{}, TaskTerminals: map[museTaskKey]string{}, TaskTerminalIndex: map[museTaskKey]int{}, TaskIdempotency: map[museTaskKey]string{}, Mirror: map[string]museTaskState{}, MirrorIdempotency: map[string]string{}, Cleanup: map[museCleanupKey]museCleanupState{}, CleanupChild: map[museCleanupKey]string{}, CleanupStartIndex: map[museCleanupKey]int{}, Intents: map[string]museIntent{}, StartedRecords: map[string]museStartedRecord{}, ObservedQueue: map[string]museObservedQueue{}, Responses: map[string]map[string]bool{}, MessageResponses: map[string]map[string]bool{}, MessageIDs: map[museMessageKey]bool{}, Terminals: map[string]museRunTerminal{}, Messages: map[string]string{}, IntakeReceived: map[string]bool{}, IntakeSettled: map[string]bool{}, SessionStartSeen: map[string]bool{}, ConfiguredSources: map[string]map[string]bool{}, ConfiguredContexts: map[string]map[string]bool{}, ConfiguredRoster: map[string]map[string]bool{}, SeenSeqs: map[int]bool{}, RecordIndex: map[string]int{}, RecordSeq: map[string]int{}}
	f.StartupPrefixWitnessed = witnessed
	f.ToolEffects = map[museToolEffectKey]museToolEffect{}
	lines, err := museLogRecords(data)
	if err != nil {
		return nil, err
	}
	// Bind the owned session before folding: a runtime record must belong
	// to the opened session as well as its run. Route facts precede the
	// opened record in every capture, so the binding resolves up front.
	bound, err := museBindSession(lines)
	if err != nil {
		return nil, err
	}
	// Forward references resolve after the fold: retained facts, task
	// links and materialized outcomes all precede their run or task in
	// the captures, so prior observation cannot be required inline.
	retainedRuns := map[string]bool{}
	linkedTasks := map[museTaskKey]bool{}
	for foldIndex, record := range lines {
		if bound != "" && record.Stream.ID != bound {
			return nil, fmt.Errorf("Muse record names a foreign session")
		}
		// Envelope identity format (r14, J50): the closed grammar
		// types the envelope id and stream as UUIDs, so an empty,
		// whitespace-only or malformed id refuses at record decode
		// — before any foreign check, before any insertion — and is
		// never stored in an index. A blank prior envelope once let
		// an empty resume citation resolve; with this gate no
		// malformed citation can ever resolve. Every captured
		// envelope carries a UUID id and stream (678/678 across the
		// corpus and both real logs).
		if record.Sequence <= 0 {
			return nil, fmt.Errorf("Muse record sequence is not positive")
		}
		// Global envelope uniqueness (r12): one set of every outer
		// record envelope id and every sequence observed in the
		// owned stream. Any repeat, anywhere, is unknown state.
		// This closes the started-envelope alias and the
		// intent-envelope alias and every alias of that shape;
		// the per-map stores below keep their own keys but no
		// identity is attested twice. Records refused above as
		// foreign never enter the set. The real captures carry no
		// repeats within a stream.
		if _, dup := f.RecordIndex[record.ID]; dup {
			return nil, fmt.Errorf("Muse record envelope id repeats")
		}
		if f.SeenSeqs[record.Sequence] {
			return nil, fmt.Errorf("Muse record sequence repeats")
		}
		f.RecordIndex[record.ID] = foldIndex
		f.RecordSeq[record.ID] = record.Sequence
		f.SeenSeqs[record.Sequence] = true
		var p struct {
			Kind              string          `json:"kind"`
			PID               int             `json:"pid"`
			SessionID         string          `json:"session_id"`
			RunID             string          `json:"run_id"`
			SourceRunRecordID string          `json:"source_run_record_id"`
			TaskID            string          `json:"task_id"`
			Event             json.RawMessage `json:"event"`
			Record            json.RawMessage `json:"record"`
		}
		if err := json.Unmarshal(record.Payload, &p); err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(record.Payload, &object); err != nil || len(object) == 0 {
			return nil, fmt.Errorf("Muse payload is not a decoded object")
		}
		// Present event/record objects cannot be null, empty, or scalar. Their
		// omission is checked by each recognized record family below.
		if raw, ok := object["record"]; ok && !museJSONObject(raw) {
			return nil, fmt.Errorf("Muse nested record is unknown")
		}
		// Any session id a record names must be the owned stream: the
		// nested record identities (opened, end, resumed, startup,
		// command, agent tree, intake session stream) and the direct
		// payload identities (name.changed, user intent source) alike.
		if err := museCheckSessionBindings(object, record.Stream.ID); err != nil {
			return nil, err
		}
		switch record.Type {
		case "session.opened.observed":
			if f.ReopenPending {
				return nil, fmt.Errorf("Muse session reopened twice without resume")
			}
			wasEnded := f.Ended
			first := !f.Opened
			// The open consumes a pending 1.4.10 session start.
			f.SessionStartPending = false
			if f.Ended {
				f.Ended = false
				f.ReopenPending = true
			}
			f.Opened = true
			f.SessionID = record.Stream.ID
			// One process binding per epoch. The first opened
			// disarms: the initial route binds through the unbound
			// path, and a mid-epoch foreign route must refuse, not
			// rebind. An end-to-opened reopen keeps the arm state
			// the end set: armed still when no route came between,
			// spent when the pre-opened route bound the new epoch.
			// Every reopen waits for the captured resume context:
			// a reopen without an end (a resume after a stop that
			// wrote no end) arms for the post-opened route AND
			// stays pending until session.resumed witnesses it. A
			// reopen that only rebinds the PID is unknown state.
			// The snapshot still refuses any pid but the owned child's.
			switch {
			case first:
				f.pidRebind = false
			case wasEnded:
			default:
				f.pidRebind = true
				f.ReopenPending = true
			}
			// Every opened starts an epoch: the resume reference must
			// resolve to a record of the epoch before this one.
			f.EpochOpens = append(f.EpochOpens, foldIndex)
		case "session.end":
			if !f.Opened || f.Ended || f.ReopenPending {
				return nil, fmt.Errorf("Muse session end outside an open session")
			}
			f.Ended = true
			// The epoch boundary is the end, not the opened: the
			// captured 1.4.1 resume order is end, route with the new
			// process, opened, resumed. The next route binds the new
			// epoch and spends the arm.
			f.pidRebind = true
		case "session.resumed":
			// The captured resume context: clears a pending reopen. A
			// resumed record outside a reopen is unknown state: no
			// complete capture shows one in a legitimate flow (the
			// lone corpus resumed is a single-record fragment
			// stream, not a foldable session).
			if !f.ReopenPending {
				return nil, fmt.Errorf("Muse session resumed without a pending reopen")
			}
			// The resume reference resolves exactly to the prior
			// epoch's recorded record (r12): the cited record id
			// was observed with the cited sequence in the epoch
			// before this reopen. An unresolved or mismatched
			// reference is unknown state. The real 1.4.1 resume
			// cites its prior run terminal. A pending reopen
			// always follows a non-first opened, so a prior epoch
			// exists whenever the orphan gate above passes.
			var ref struct {
				RecordID string `json:"resumed_from_record_id"`
				Sequence int    `json:"resumed_from_sequence"`
			}
			if err := json.Unmarshal(p.Record, &ref); err != nil {
				return nil, err
			}
			if len(f.EpochOpens) >= 2 {
				// A malformed citation names no observed record: the
				// grammar never admits a malformed id into the fold,
				// so the lookup below misses it like any other
				// unobserved id. No separate malformed gate exists
				// to narrow: a citation resolves only against a
				// stored identity.
				idx, ok := f.RecordIndex[ref.RecordID]
				if !ok {
					return nil, fmt.Errorf("Muse resumed reference names an unobserved record")
				}
				if f.RecordSeq[ref.RecordID] != ref.Sequence {
					return nil, fmt.Errorf("Muse resumed reference sequence differs from the recorded sequence")
				}
				prior := len(f.EpochOpens) - 2
				epoch := 0
				for k, opened := range f.EpochOpens {
					if opened <= idx {
						epoch = k
					}
				}
				if epoch != prior {
					return nil, fmt.Errorf("Muse resumed reference names a record outside the prior epoch")
				}
			}
			f.ReopenPending = false
		case "runtime.session.route_facts":
			var route struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal(p.Record, &route); err != nil {
				return nil, err
			}
			if route.PID <= 0 {
				return nil, fmt.Errorf("Muse route has no process identity")
			}
			// A route restating the bound process is idempotent. An
			// armed rebind (after an end, or after a reopen without
			// one) or the still-unbound initial epoch binds and
			// spends the arm. Anything else is another process
			// claiming the live epoch and refuses.
			switch {
			case f.PID != 0 && route.PID == f.PID:
			case f.pidRebind || f.PID == 0:
				f.PID = route.PID
				f.pidRebind = false
			default:
				return nil, fmt.Errorf("Muse route claims another process in this session epoch")
			}
		case "runtime.session.capture_selection":
			// Version-1 capture publication is session metadata, not run
			// work. Both identities repeat the owning envelope in 1.4.7.
			var selection struct {
				SessionStream struct {
					ID string `json:"id"`
				} `json:"session_stream"`
				PublicationID string `json:"publication_id"`
			}
			if err := json.Unmarshal(record.Payload, &selection); err != nil {
				return nil, err
			}
			if selection.SessionStream.ID != record.Stream.ID || selection.PublicationID != record.ID {
				return nil, fmt.Errorf("Muse capture selection names a foreign publication or session")
			}
		case "tool_batch.effect.started", "tool_batch.effect.terminal":
			if err := museRequireOpenEpoch(f, "tool effect", p.RunID); err != nil {
				return nil, err
			}
			var effect struct {
				ID         string `json:"effect_id"`
				Task       string `json:"task_id"`
				Call       string `json:"call_id"`
				ModelIndex int    `json:"model_call_index"`
				Stream     struct {
					ID string `json:"id"`
				} `json:"task_stream"`
			}
			if err := json.Unmarshal(p.Record, &effect); err != nil {
				return nil, err
			}
			if f.RunLifecycle[p.RunID] != museRunActive || f.Tasks[museTaskKey{Run: p.RunID, Task: effect.Task}] != museTaskStarted || effect.Stream.ID != effect.Task {
				return nil, fmt.Errorf("Muse tool effect lacks active run/task binding")
			}
			key := museToolEffectKey{Run: p.RunID, Effect: effect.ID}
			prior, exists := f.ToolEffects[key]
			if record.Type == "tool_batch.effect.started" {
				if exists {
					return nil, fmt.Errorf("Muse tool effect started twice")
				}
				f.ToolEffects[key] = museToolEffect{Task: effect.Task, Call: effect.Call, ModelIndex: effect.ModelIndex}
			} else {
				if !exists || prior.Terminal || prior.Task != effect.Task || prior.Call != effect.Call || prior.ModelIndex != effect.ModelIndex {
					return nil, fmt.Errorf("Muse tool effect terminal lacks matching start")
				}
				prior.Terminal = true
				f.ToolEffects[key] = prior
			}
		case "runtime.session":
			if p.Kind == "security_mode" {
				if err := museRequireOpenEpoch(f, "security mode", ""); err != nil {
					return nil, err
				}
				continue
			}
			if p.Kind == "agent_tree_initialized" {
				if err := museRequireOpenEpoch(f, "agent tree", ""); err != nil {
					return nil, err
				}
				var tree map[string]json.RawMessage
				if json.Unmarshal(p.Record, &tree) != nil || len(tree) == 0 {
					return nil, fmt.Errorf("Muse agent tree record is unknown")
				}
				continue
			}
			if p.Kind != "run" && p.Kind != "task" {
				return nil, fmt.Errorf("unknown Muse runtime record kind %q", p.Kind)
			}
			// The entire record, including this event, has already crossed
			// decodeMuseLogRecord's recursive known-field boundary.
			var e struct {
				Kind           string          `json:"kind"`
				RunID          string          `json:"run_id"`
				TaskID         string          `json:"task_id"`
				ItemID         string          `json:"item_id"`
				Terminal       string          `json:"terminal"`
				Text           string          `json:"text"`
				MessageID      string          `json:"message_id"`
				ResponseID     string          `json:"response_id"`
				IdempotencyKey string          `json:"idempotency_key"`
				Prompt         *string         `json:"prompt"`
				Target         json.RawMessage `json:"drain_target_run_stream"`
				Snapshot       json.RawMessage `json:"delivery_snapshot"`
			}
			if err := json.Unmarshal(p.Event, &e); err != nil {
				return nil, err
			}
			if e.Kind == "" {
				return nil, fmt.Errorf("Muse runtime event is unknown")
			}
			if p.Kind == "task" {
				// Task records join the run fold: identity, an ACTIVE
				// run, and the captured lifecycle edge are proven
				// before any state effect. A task under an unobserved
				// or terminated run, a duplicate or reordered event,
				// and any event after the key's terminal are all
				// unknown state.
				if err := museRequireOpenEpoch(f, "task", p.TaskID); err != nil {
					return nil, err
				}
				if e.TaskID != p.TaskID || (!museTaskEvent(e.Kind) && e.Kind != "status" && e.Kind != "output") {
					return nil, fmt.Errorf("unknown Muse task event")
				}
				if f.RunLifecycle[p.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse task names a run with no active lifecycle")
				}
				key := museTaskKey{Run: p.RunID, Task: p.TaskID}
				if e.Kind == "status" || e.Kind == "output" {
					// Captured live task diagnostics are self-loops only
					// after started, never evidence of task completion.
					if f.Tasks[key] != museTaskStarted {
						return nil, fmt.Errorf("Muse task diagnostic lacks active task")
					}
					continue
				}
				next, ok := museTaskTransitions[f.Tasks[key]][e.Kind]
				if !ok {
					return nil, fmt.Errorf("Muse task transition %q is not captured", e.Kind)
				}
				// The scheduled effect and its side-effect
				// intent carry the same idempotency identity
				// (19/19 captured pairs, both scopes). A
				// mismatch or a missing key is unknown state.
				switch e.Kind {
				case "scheduled":
					f.TaskIdempotency[key] = e.IdempotencyKey
				case "side_effect_intent":
					if e.IdempotencyKey != f.TaskIdempotency[key] {
						return nil, fmt.Errorf("Muse task side effect names another idempotency identity")
					}
				}
				f.Tasks[key] = next
				if next == museTaskTerminated {
					// The terminal kind is preserved: a cleanup
					// terminal later reports this bound child's
					// outcome and must name exactly this kind.
					// The terminal settles only its own exact
					// key: a run-bound terminal never discharges
					// a mirror, which waits for its own terminal.
					f.TaskTerminals[key] = e.Kind
					// The terminal position is preserved too: a
					// cleanup terminal reports a child terminal
					// recorded after its own start.
					f.TaskTerminalIndex[key] = foldIndex
				}
				continue
			}
			if e.RunID != "" && e.RunID != p.RunID {
				return nil, fmt.Errorf("Muse run identity is unknown or inconsistent")
			}
			if e.RunID == "" {
				e.RunID = p.RunID
			}
			// Every run event below — started, terminal, messages,
			// queue, drain, configured identities, traces,
			// diagnostics, links and the auxiliary self-loops — is
			// admitted only in an open epoch (r13, J49).
			if err := museRequireOpenEpoch(f, "run", e.RunID); err != nil {
				return nil, err
			}
			switch {
			case p.Kind == "run" && e.Kind == "started":
				if e.Prompt == nil {
					return nil, fmt.Errorf("Muse started run has no identity")
				}
				// The started source is stored per run and cited by
				// intent outcomes (J36). Format is a grammar type
				// (r14, J51): all 13 captured started sources are
				// UUIDs, and a malformed source refuses at decode.
				switch f.RunLifecycle[e.RunID] {
				case museRunActive:
					return nil, fmt.Errorf("Muse run started twice without terminal")
				case museRunTerminated:
					return nil, fmt.Errorf("Muse run restarted after its terminal")
				}
				f.RunLifecycle[e.RunID] = museRunActive
				f.Started = append(f.Started, e.RunID)
				// The started record identity an intent outcome
				// must name: the outer record id and the source
				// run record id, 5/5 exact in the captures.
				f.StartedRecords[e.RunID] = museStartedRecord{ID: record.ID, SourceID: p.SourceRunRecordID, Index: foldIndex}
			case p.Kind == "run" && e.Kind == "assistant_message_committed":
				var fields map[string]json.RawMessage
				_ = json.Unmarshal(p.Event, &fields)
				if text, ok := fields["text"]; !ok || bytes.Equal(bytes.TrimSpace(text), []byte("null")) {
					return nil, fmt.Errorf("Muse message text is unknown")
				}
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse message lacks correlated start")
				}
				mkey := museMessageKey{Run: e.RunID, ID: e.MessageID}
				if f.MessageIDs[mkey] {
					return nil, fmt.Errorf("Muse message committed twice")
				}
				f.MessageIDs[mkey] = true
				// Every response a message names is recorded
				// for the post-fold reconciliation: a created
				// record landing after the message binds it
				// just the same.
				if f.MessageResponses[e.RunID] == nil {
					f.MessageResponses[e.RunID] = map[string]bool{}
				}
				f.MessageResponses[e.RunID][e.ResponseID] = true
				// A message referring to an observed model
				// response matches that response in its own run
				// (5/5 in the captures). A run that observed no
				// response carries no witness to match: the
				// sixth captured message commits without one,
				// and no missing-record requirement is invented.
				if len(f.Responses[e.RunID]) > 0 && !f.Responses[e.RunID][e.ResponseID] {
					return nil, fmt.Errorf("Muse message names an unobserved response")
				}
				if f.Messages[e.RunID] != "" {
					f.Messages[e.RunID] += "\n"
				}
				f.Messages[e.RunID] += e.Text
			case p.Kind == "run" && e.Kind == "model_response_created":
				// A created response binds its run and its
				// response id: one creation per (run, response)
				// in every capture, and every later message or
				// reasoning entry in that run names it.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse response lacks correlated start")
				}
				if f.Responses[e.RunID] == nil {
					f.Responses[e.RunID] = map[string]bool{}
				}
				if f.Responses[e.RunID][e.ResponseID] {
					return nil, fmt.Errorf("Muse response created twice")
				}
				f.Responses[e.RunID][e.ResponseID] = true
			case p.Kind == "run" && e.Kind == "reasoning_committed":
				// Reasoning commits under the same response
				// identity as messages: 2/2 captured entries
				// name their run's created response. Its text is
				// model-internal content with no M2 consumer, so
				// only the identity joins the fold.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse reasoning lacks correlated start")
				}
				rkey := museMessageKey{Run: e.RunID, ID: e.MessageID}
				if f.MessageIDs[rkey] {
					return nil, fmt.Errorf("Muse reasoning committed twice")
				}
				f.MessageIDs[rkey] = true
				if f.MessageResponses[e.RunID] == nil {
					f.MessageResponses[e.RunID] = map[string]bool{}
				}
				f.MessageResponses[e.RunID][e.ResponseID] = true
				if len(f.Responses[e.RunID]) > 0 && !f.Responses[e.RunID][e.ResponseID] {
					return nil, fmt.Errorf("Muse reasoning names an unobserved response")
				}
			case p.Kind == "run" && e.Kind == "terminal":
				if e.Terminal == "" {
					return nil, fmt.Errorf("Muse terminal outcome is unknown")
				}
				if e.Terminal != "completed" {
					// Preserve the failure result for M2, but a future terminal
					// cannot certify that the TUI has no remaining work.
					f.Unknown = true
				}
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse terminal lacks correlated start")
				}
				f.RunLifecycle[e.RunID] = museRunTerminated
				terminal := museRunTerminal{RunID: e.RunID, Terminal: e.Terminal, Text: f.Messages[e.RunID]}
				f.Terminals[e.RunID] = terminal
			case e.Kind == "inbox_item_queued":
				// The queue forward identity is stored with the item
				// and cited by drains (J17) and deliveries (J37b).
				// Format is a grammar type (r14, J51): both captured
				// queue causations are UUIDs, and a malformed
				// causation refuses at decode.
				if f.RunLifecycle[p.RunID] != museRunActive {
					return nil, fmt.Errorf("Muse queue names a run with no active lifecycle")
				}
				if _, pending := f.Queued[p.SourceRunRecordID]; pending || f.QueueDrained[p.SourceRunRecordID] {
					return nil, fmt.Errorf("Muse inbox item queued twice")
				}
				content, err := museQueueContent(p.Event)
				if err != nil {
					return nil, err
				}
				f.Queued[p.SourceRunRecordID] = museQueuedItem{Run: p.RunID, Causation: museCausationID(record.Causation), Content: content}
				// The envelope identifies the queued item: a later
				// queue reusing an observed envelope for another
				// source item aliases the observation and is
				// unknown state. A same-source duplicate refuses
				// at the item gate above, so the observation is
				// never overwritten here either way.
				// Global envelope uniqueness fires before this join can
				// mismatch (r12); it stays as defense in depth behind
				// the store the drain arm resolves.
				if prev, dup := f.ObservedQueue[record.ID]; dup && prev.SourceID != p.SourceRunRecordID {
					return nil, fmt.Errorf("Muse queue reuses an observed envelope for another item")
				}
				// The queue observation an active-run delivery
				// must cite: it resolves whatever the drain
				// state, so the drain never removes it.
				if _, dup := f.ObservedQueue[record.ID]; !dup {
					f.ObservedQueue[record.ID] = museObservedQueue{Causation: museCausationID(record.Causation), Run: p.RunID, SourceID: p.SourceRunRecordID, Index: foldIndex}
				}
			case e.Kind == "inbox_item_drained":
				item, pending := f.Queued[e.ItemID]
				if !pending {
					return nil, fmt.Errorf("Muse drained inbox item lacks correlated queue")
				}
				var target struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(e.Target, &target); err != nil {
					return nil, err
				}
				// The captures relate the drain target to its run by
				// equality (2/2); anything else is unknown state.
				if target.ID != p.RunID {
					return nil, fmt.Errorf("Muse drain target names another run")
				}
				// Both ends of the drain join the owned stream: the
				// queue run started before its queue, and the target
				// equals the stored forward identity. A known but
				// unrelated target is not a discharge.
				if !museRunKnown(f, item.Run) {
					return nil, fmt.Errorf("Muse drain names an unobserved queue run")
				}
				if item.Causation == "" || item.Causation != target.ID {
					return nil, fmt.Errorf("Muse drain target differs from the queued forward reference")
				}
				var snapshot map[string]any
				if err := json.Unmarshal(e.Snapshot, &snapshot); err != nil {
					return nil, err
				}
				raw, err := json.Marshal(snapshot)
				if err != nil {
					return nil, err
				}
				if !bytes.Equal(raw, item.Content) {
					return nil, fmt.Errorf("Muse drain snapshot differs from the queued item")
				}
				delete(f.Queued, e.ItemID)
				f.QueueDrained[e.ItemID] = true
				// The forward reference alone is not proof the target
				// run has no work: a target the owned stream never
				// started stays pending until its run is observed.
				// The captured forward-only drain keeps its wire shape
				// and folds without error; it just never certifies an
				// open host idle.
				if !museRunKnown(f, target.ID) {
					f.DrainTargetPending[target.ID] = true
				}
			case e.Kind == "model_request_configured":
				// The run's configured request identity: later
				// traces, diagnostics and reminder links resolve
				// against it after the fold, in any arrival order.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				// Every configured identity is stored per run and
				// cited after the fold (J44-J46). Format is a grammar
				// type (r14, J51): all 12 captured sources are UUIDs,
				// all 132 captured context ids name the 11 blocks,
				// all 22 captured roster ids name the 2 agents, and a
				// malformed id refuses at decode.
				var cfg struct {
					Messages []struct {
						ID string `json:"id"`
					} `json:"run_context_messages"`
					Roster struct {
						Agents []struct {
							ID string `json:"id"`
						} `json:"agents"`
					} `json:"reminder_roster"`
				}
				if err := json.Unmarshal(p.Event, &cfg); err != nil {
					return nil, err
				}
				if f.ConfiguredSources[e.RunID] == nil {
					f.ConfiguredSources[e.RunID] = map[string]bool{}
				}
				f.ConfiguredSources[e.RunID][p.SourceRunRecordID] = true
				if f.ConfiguredContexts[e.RunID] == nil {
					f.ConfiguredContexts[e.RunID] = map[string]bool{}
				}
				for _, m := range cfg.Messages {
					f.ConfiguredContexts[e.RunID][m.ID] = true
				}
				if f.ConfiguredRoster[e.RunID] == nil {
					f.ConfiguredRoster[e.RunID] = map[string]bool{}
				}
				for _, a := range cfg.Roster.Agents {
					f.ConfiguredRoster[e.RunID][a.ID] = true
				}
			case e.Kind == "model_input_trace_recorded":
				// The trace cites its run's configured request
				// source (6/6 in the captures); the reference
				// resolves after the fold.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				var trace struct {
					Request string `json:"request_record_id"`
				}
				if err := json.Unmarshal(p.Event, &trace); err != nil {
					return nil, err
				}
				f.TraceRefs = append(f.TraceRefs, museAuxRef{Run: e.RunID, ID: trace.Request})
			case e.Kind == "context_block_diagnostic":
				// The diagnostic cites a configured context block
				// of its own run (66/66); diagnostics precede
				// the configuration in the captures, so the
				// reference resolves after the fold.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				var diag struct {
					Block string `json:"block_id"`
				}
				if err := json.Unmarshal(p.Event, &diag); err != nil {
					return nil, err
				}
				f.DiagRefs = append(f.DiagRefs, museAuxRef{Run: e.RunID, ID: diag.Block})
			case e.Kind == "memory_reminder_child_session_linked":
				// The link cites a member of its run's configured
				// reminder roster (10/10) as well as the exact
				// (run, task) lifecycle; both resolve after the
				// fold.
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				if err := museCheckAuxIdentity(e.Kind, record.Stream.ID, p.RunID, p.Event); err != nil {
					return nil, err
				}
				var link struct {
					Agent string `json:"reminder_agent_id"`
				}
				if err := json.Unmarshal(p.Event, &link); err != nil {
					return nil, err
				}
				f.ReminderRefs = append(f.ReminderRefs, museAuxRef{Run: e.RunID, ID: link.Agent})
				// A link resolves to the exact (run, task)
				// lifecycle it names: the same task id under
				// another run is a different lifecycle, never a
				// discharge.
				linkedTasks[museTaskKey{Run: p.RunID, Task: e.TaskID}] = true
			default:
				// Only observed auxiliary run events may leave state unchanged,
				// and only inside a correlated active run. Future events are unknown.
				if !museRunAuxEvents[e.Kind] {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				if f.RunLifecycle[e.RunID] != museRunActive {
					return nil, fmt.Errorf("unknown or uncorrelated Muse run event %q", e.Kind)
				}
				if err := museCheckAuxIdentity(e.Kind, record.Stream.ID, p.RunID, p.Event); err != nil {
					return nil, err
				}
				if e.Kind == "task_stream_linked" {
					// A link resolves to the exact (run, task)
					// lifecycle it names: the same task id under
					// another run is a different lifecycle, never a
					// discharge.
					linkedTasks[museTaskKey{Run: p.RunID, Task: e.TaskID}] = true
				}
			}
		case "runtime.session.task":
			// Task-stream mirrors name session-scoped work. A mirror
			// follows the same captured lifecycle; any planning
			// prefix without its own terminal stays pending until
			// the mirror's own terminal settles it. A complete
			// mirror lifecycle discharges itself: the pinned TUI
			// writes one at every startup before any run exists,
			// so a counterpart-free mirror terminal is fresh-TUI
			// idle, not missing work. A run-bound terminal never
			// settles a mirror: mirror and run-bound task ids are
			// disjoint in every capture, so no shared key exists
			// to join on.
			if err := museRequireOpenEpoch(f, "mirror", p.TaskID); err != nil {
				return nil, err
			}
			// L3G-004: mirror events project like run events, so
			// additive members cannot fail this decode.
			var mirror struct {
				Kind           string `json:"kind"`
				TaskID         string `json:"task_id"`
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.Unmarshal(p.Event, &mirror); err != nil {
				return nil, err
			}
			if mirror.TaskID != p.TaskID || !museTaskEvent(mirror.Kind) {
				return nil, fmt.Errorf("Muse mirror task identity is unknown")
			}
			next, ok := museTaskTransitions[f.Mirror[p.TaskID]][mirror.Kind]
			if !ok {
				return nil, fmt.Errorf("Muse mirror transition %q is not captured", mirror.Kind)
			}
			// Same idempotency join as run-bound tasks, on
			// the session-scoped mirror key.
			switch mirror.Kind {
			case "scheduled":
				f.MirrorIdempotency[p.TaskID] = mirror.IdempotencyKey
			case "side_effect_intent":
				if mirror.IdempotencyKey != f.MirrorIdempotency[p.TaskID] {
					return nil, fmt.Errorf("Muse mirror side effect names another idempotency identity")
				}
			}
			f.Mirror[p.TaskID] = next
		case "runtime.retained_fact":
			if err := museRequireOpenEpoch(f, "retained fact", p.RunID); err != nil {
				return nil, err
			}
			retainedRuns[p.RunID] = true
		case "runtime.user_intent.accepted":
			// An accepted intent stays pending until its outcome
			// references the stored intent. The captures pair
			// every accepted record with its materialized outcome
			// by intent id (6/6), and the outcome names the
			// accepted envelope identity (6/6).
			raw, ok := object["intent_id"]
			var intentID string
			if !ok || json.Unmarshal(raw, &intentID) != nil {
				return nil, fmt.Errorf("Muse accepted intent names no intent")
			}
			if err := museRequireOpenEpoch(f, "intent", intentID); err != nil {
				return nil, err
			}
			if _, dup := f.Intents[intentID]; dup {
				return nil, fmt.Errorf("Muse intent accepted twice")
			}
			f.Intents[intentID] = museIntent{State: museIntentAccepted, RecordID: record.ID, Sequence: record.Sequence, AcceptIndex: foldIndex}
		case "runtime.user_intent.materialized":
			raw, ok := object["intent_id"]
			var intentID string
			if !ok || json.Unmarshal(raw, &intentID) != nil {
				return nil, fmt.Errorf("Muse materialized intent names no intent")
			}
			if err := museRequireOpenEpoch(f, "intent", intentID); err != nil {
				return nil, err
			}
			var mat struct {
				EnvelopeID  string `json:"envelope_record_id"`
				EnvelopeSeq int    `json:"envelope_sequence"`
				Outcome     struct {
					Kind              string `json:"kind"`
					RunID             string `json:"run_id"`
					StartedSessionRec string `json:"run_started_session_record_id"`
					StartedSourceRec  string `json:"run_started_source_record_id"`
					DeliveryRec       string `json:"delivery_record_id"`
				} `json:"outcome"`
			}
			if err := json.Unmarshal(record.Payload, &mat); err != nil {
				return nil, err
			}
			state, ok := f.Intents[intentID]
			if !ok {
				return nil, fmt.Errorf("Muse materialized intent lacks an accepted intent")
			}
			if state.State == museIntentMaterialized {
				return nil, fmt.Errorf("Muse intent materialized twice")
			}
			// The outcome references the stored intent: the
			// exact accepted envelope id and sequence (6/6).
			if mat.EnvelopeID != state.RecordID || mat.EnvelopeSeq != state.Sequence {
				return nil, fmt.Errorf("Muse materialized intent names another envelope")
			}
			// The outcome kind is the captured kind: a
			// top-level turn names the run the intent produced,
			// an active delivery names its target run and the
			// queued record it delivers.
			switch mat.Outcome.Kind {
			case "top_level_turn_started", "active_run_delivery":
			default:
				return nil, fmt.Errorf("Muse materialized intent names an unknown outcome")
			}
			state.State = museIntentMaterialized
			f.Intents[intentID] = state
			// The run and record references resolve after the
			// fold: the outcome may precede its run's start.
			f.PendingOutcomes = append(f.PendingOutcomes, musePendingOutcome{Intent: intentID, Kind: mat.Outcome.Kind, Run: mat.Outcome.RunID, StartedSessionRec: mat.Outcome.StartedSessionRec, StartedSourceRec: mat.Outcome.StartedSourceRec, DeliveryRec: mat.Outcome.DeliveryRec})
		case "runtime.command_intake.received", "runtime.command_intake.session_name.received":
			// A received command registers its command id (8/8
			// captured receives settle exactly once, received
			// before settled, both spellings alike).
			var rec struct {
				CommandID string `json:"command_id"`
				Command   struct {
					Kind string `json:"kind"`
				} `json:"command"`
			}
			if err := json.Unmarshal(p.Record, &rec); err != nil {
				return nil, err
			}
			if record.Type == "runtime.command_intake.received" && rec.Command.Kind == "session_start" {
				// Muse 1.4.10 receives the session-start command
				// before the session opens. Captured orders: intake,
				// intake_v3, started, opened, settled at startup;
				// end, route, intake, intake_v3, started, opened,
				// resumed, settled at resume. It is the only intake
				// admitted outside an open epoch: once before the
				// first open, or once after an end and before the
				// reopen. A start inside an open epoch has no
				// capture and stays unknown state.
				if epoch := f.epoch(); (epoch != museEpochNone && epoch != museEpochEnded) || f.SessionStartPending {
					return nil, fmt.Errorf("Muse session start %q outside a startup or resume", rec.CommandID)
				}
				f.SessionStartPending = true
				f.SessionStartCommand = rec.CommandID
				f.SessionStartSeen = map[string]bool{}
			} else if err := museRequireOpenEpoch(f, "intake", rec.CommandID); err != nil {
				return nil, err
			}
			if f.IntakeReceived[rec.CommandID] {
				return nil, fmt.Errorf("Muse intake command received twice")
			}
			f.IntakeReceived[rec.CommandID] = true
		case "runtime.session_start.intake_v3", "session.started":
			// Both name the pending session-start command and
			// precede its open in the 1.4.10 captures; each is
			// recorded once per start.
			var rec struct {
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal(record.Payload, &rec); err != nil {
				return nil, err
			}
			if !f.SessionStartPending || rec.CommandID != f.SessionStartCommand {
				return nil, fmt.Errorf("Muse %s names no pending session start", record.Type)
			}
			if f.SessionStartSeen[record.Type] {
				return nil, fmt.Errorf("Muse %s recorded twice", record.Type)
			}
			f.SessionStartSeen[record.Type] = true
		case "runtime.command_intake.settled":
			// A settled record resolves to its received command:
			// a settled record for another command is unknown
			// state (8/8 captured settles cite a received id).
			var rec struct {
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal(p.Record, &rec); err != nil {
				return nil, err
			}
			if err := museRequireOpenEpoch(f, "intake", rec.CommandID); err != nil {
				return nil, err
			}
			if !f.IntakeReceived[rec.CommandID] {
				return nil, fmt.Errorf("Muse intake settled names an unreceived command")
			}
			if f.IntakeSettled[rec.CommandID] {
				return nil, fmt.Errorf("Muse intake command settled twice")
			}
			f.IntakeSettled[rec.CommandID] = true
		case "reminder.cleanup_effect.started":
			var rec struct {
				EffectID  int `json:"cleanup_effect_id"`
				ParentRun struct {
					ID string `json:"id"`
				} `json:"parent_run_stream"`
				Child struct {
					TaskID string `json:"task_id"`
					Stream struct {
						ID string `json:"id"`
					} `json:"task_stream"`
				} `json:"child_task"`
			}
			if err := json.Unmarshal(p.Record, &rec); err != nil {
				return nil, err
			}
			if err := museRequireOpenEpoch(f, "cleanup", p.RunID); err != nil {
				return nil, err
			}
			// The effect id keys the (run, effect) pair the terminal
			// must cite: a non-positive id is unknown state and is
			// never stored (r13, J51). All 6 captured effect ids
			// read 1.
			if rec.EffectID <= 0 {
				return nil, fmt.Errorf("Muse cleanup effect lacks identity")
			}
			if rec.ParentRun.ID != p.RunID || rec.Child.Stream.ID != rec.Child.TaskID {
				return nil, fmt.Errorf("Muse cleanup names another run or task")
			}
			if !museRunKnown(f, p.RunID) {
				return nil, fmt.Errorf("Muse cleanup names an unobserved run or task")
			}
			childKey := museTaskKey{Run: p.RunID, Task: rec.Child.TaskID}
			if _, ok := f.Tasks[childKey]; !ok {
				return nil, fmt.Errorf("Muse cleanup names an unobserved run or task")
			}
			// The cleanup starts while its child is still live:
			// all three captures order the child terminal after
			// the start. An already terminated child is unknown
			// state, not a discharge.
			if f.Tasks[childKey] == museTaskTerminated {
				return nil, fmt.Errorf("Muse cleanup starts after its child terminal")
			}
			key := museCleanupKey{Run: p.RunID, Effect: rec.EffectID}
			if _, dup := f.Cleanup[key]; dup {
				return nil, fmt.Errorf("Muse cleanup effect started twice")
			}
			f.Cleanup[key] = museCleanupStarted
			// The start binds the child task the terminal must
			// report: 3/3 captured pairs name a child of the
			// parent run that is proposed before the start.
			f.CleanupChild[key] = rec.Child.TaskID
			f.CleanupStartIndex[key] = foldIndex
		case "reminder.cleanup_effect.terminal":
			var rec struct {
				EffectID int `json:"cleanup_effect_id"`
				Outcome  struct {
					Child struct {
						Kind string `json:"kind"`
					} `json:"child_terminal"`
				} `json:"outcome"`
			}
			if err := json.Unmarshal(p.Record, &rec); err != nil {
				return nil, err
			}
			if err := museRequireOpenEpoch(f, "cleanup", p.RunID); err != nil {
				return nil, err
			}
			// The terminal lands inside the parent run's life:
			// all three captures order it before the parent
			// terminal. A terminated parent is unknown state,
			// not a discharge.
			if f.RunLifecycle[p.RunID] != museRunActive {
				return nil, fmt.Errorf("Muse cleanup terminal names a run with no active lifecycle")
			}
			// A terminal discharges only the started effect with its
			// exact (run, effect) key: a terminal without a start, a
			// mismatched id, and a duplicate terminal are all unknown
			// state. The captures hold three correctly paired effects.
			key := museCleanupKey{Run: p.RunID, Effect: rec.EffectID}
			state, ok := f.Cleanup[key]
			if !ok {
				return nil, fmt.Errorf("Muse cleanup terminal lacks a started effect")
			}
			if state == museCleanupTerminated {
				return nil, fmt.Errorf("Muse cleanup terminal repeated")
			}
			// The terminal reports the bound child's outcome: the
			// child the start named must have terminated with
			// exactly the claimed kind. All three captures cancel
			// the child before the cleanup reports cancellation.
			child := f.CleanupChild[key]
			if kind, ok := f.TaskTerminals[museTaskKey{Run: p.RunID, Task: child}]; !ok || kind != rec.Outcome.Child.Kind {
				return nil, fmt.Errorf("Muse cleanup child terminal differs from the bound child")
			}
			// The child terminal the cleanup reports was recorded
			// after the cleanup start: the captured order is
			// start, child terminal, cleanup terminal (3/3).
			if f.TaskTerminalIndex[museTaskKey{Run: p.RunID, Task: child}] <= f.CleanupStartIndex[key] {
				return nil, fmt.Errorf("Muse cleanup terminal predates its child terminal")
			}
			f.Cleanup[key] = museCleanupTerminated
		default:
			// The closed grammar validated the whole metadata record before folding.
		}
	}
	// Response identity is order-independent: a run that witnessed
	// created responses binds every committed message and reasoning
	// entry to a witnessed response, whenever each record landed. A
	// run with no created response carries no witness (the captured
	// message-without-created shape), and no missing record is
	// required of it.
	for run, named := range f.MessageResponses {
		if len(f.Responses[run]) == 0 {
			continue
		}
		for id := range named {
			if !f.Responses[run][id] {
				return nil, fmt.Errorf("Muse committed response lacks a witnessed creation")
			}
		}
	}
	for id := range retainedRuns {
		if !museRunKnown(f, id) {
			return nil, fmt.Errorf("Muse retained fact names an unobserved run")
		}
	}
	for _, o := range f.PendingOutcomes {
		switch o.Kind {
		case "top_level_turn_started":
			// The intent produced this run: the outcome names
			// the intent's own run (5/5), the started record
			// it cites (5/5 exact session and source ids), and
			// that start lands after the acceptance (5/5). Any
			// other known run is unknown state, not the
			// intent's run.
			if o.Run != o.Intent {
				return nil, fmt.Errorf("Muse top-level outcome names another run")
			}
			started, ok := f.StartedRecords[o.Run]
			if !ok || started.ID != o.StartedSessionRec || started.SourceID != o.StartedSourceRec {
				return nil, fmt.Errorf("Muse top-level outcome names an unobserved start")
			}
			if started.Index <= f.Intents[o.Intent].AcceptIndex {
				return nil, fmt.Errorf("Muse top-level run started before its intent")
			}
		case "active_run_delivery":
			// The delivery cites the queue of its own intent
			// and run: the captured materialization has
			// queue.causation_id = intent_id and
			// queue.payload.run_id = outcome.run_id (1/1),
			// and the queue lands after the acceptance it
			// drains for. A known run plus an unrelated
			// queue record is unknown state.
			if !museRunKnown(f, o.Run) {
				return nil, fmt.Errorf("Muse delivery names an unobserved run")
			}
			if q, ok := f.ObservedQueue[o.DeliveryRec]; !ok {
				return nil, fmt.Errorf("Muse delivery names an unobserved queue record")
			} else if q.Causation == "" || q.Causation != o.Intent {
				return nil, fmt.Errorf("Muse delivery names a queue of another intent")
			} else if q.Run != o.Run {
				return nil, fmt.Errorf("Muse delivery names a queue of another run")
			} else if q.Index <= f.Intents[o.Intent].AcceptIndex {
				return nil, fmt.Errorf("Muse delivery queue predates its intent")
			}
		}
	}
	for key := range linkedTasks {
		if _, ok := f.Tasks[key]; !ok {
			return nil, fmt.Errorf("Muse task link names an unobserved task")
		}
	}
	// Auxiliary references resolve against the run's configured
	// identities, in any arrival order: diagnostics precede their
	// configuration in the captures, so inline observation cannot be
	// required.
	for _, ref := range f.TraceRefs {
		if !f.ConfiguredSources[ref.Run][ref.ID] {
			return nil, fmt.Errorf("Muse trace names an unconfigured request")
		}
	}
	for _, ref := range f.DiagRefs {
		if !f.ConfiguredContexts[ref.Run][ref.ID] {
			return nil, fmt.Errorf("Muse context diagnostic names an unconfigured block")
		}
	}
	for _, ref := range f.ReminderRefs {
		if !f.ConfiguredRoster[ref.Run][ref.ID] {
			return nil, fmt.Errorf("Muse reminder link names an unconfigured agent")
		}
	}
	for key, state := range f.Cleanup {
		if state == museCleanupStarted {
			return nil, fmt.Errorf("Muse cleanup effect %d in run %s lacks its terminal", key.Effect, key.Run)
		}
	}
	// A drain target observed anywhere in the owned stream settles its
	// forward reference, whenever it started. A target that never starts
	// stays pending: the drain is discharged, but the host is not idle.
	for target := range f.DrainTargetPending {
		if museRunKnown(f, target) {
			delete(f.DrainTargetPending, target)
		}
	}
	return f, nil
}

// museQueueContent canonicalizes the queued payload the drain snapshot must
// reproduce: source, summary, body, payload and disposition.
func museQueueContent(event json.RawMessage) ([]byte, error) {
	var e struct {
		Source      json.RawMessage `json:"source"`
		Summary     *string         `json:"summary"`
		Body        *string         `json:"body"`
		Payload     json.RawMessage `json:"payload"`
		Disposition *string         `json:"disposition"`
	}
	if err := json.Unmarshal(event, &e); err != nil {
		return nil, err
	}
	if e.Source == nil || e.Summary == nil || e.Body == nil || e.Payload == nil || e.Disposition == nil {
		return nil, fmt.Errorf("Muse queued inbox item lacks content")
	}
	var source, payload any
	if err := json.Unmarshal(e.Source, &source); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"source": source, "summary": *e.Summary, "body": *e.Body, "payload": payload, "disposition": *e.Disposition})
}

func museCausationID(raw json.RawMessage) string {
	var id string
	if json.Unmarshal(raw, &id) != nil {
		return ""
	}
	return id
}

// Every session reference a record carries must name the owned stream:
// the nested record.session_id and record.root_session_id, the intake
// record.session_stream.id, and the direct payload.session_id and
// payload.source_session_id.
func museCheckSessionBindings(object map[string]json.RawMessage, stream string) error {
	check := func(raw json.RawMessage) error {
		var id string
		if err := json.Unmarshal(raw, &id); err != nil || id != stream {
			return fmt.Errorf("Muse record names another session")
		}
		return nil
	}
	if raw, ok := object["record"]; ok {
		var record map[string]json.RawMessage
		if json.Unmarshal(raw, &record) == nil {
			for _, key := range []string{"session_id", "root_session_id"} {
				if raw, ok := record[key]; ok {
					if err := check(raw); err != nil {
						return err
					}
				}
			}
			if raw, ok := record["session_stream"]; ok {
				var ref struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(raw, &ref); err != nil || ref.ID != stream {
					return fmt.Errorf("Muse intake names another session")
				}
			}
		}
	}
	for _, key := range []string{"session_id", "source_session_id"} {
		if raw, ok := object[key]; ok {
			if err := check(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// Auxiliary run events name their session, run and task stream. The child
// session of a reminder link is foreign by design and never checked here.
func museCheckAuxIdentity(kind, stream, run string, event json.RawMessage) error {
	var e struct {
		TaskID        string `json:"task_id"`
		ParentSession string `json:"parent_session_id"`
		ParentRun     string `json:"parent_run_id"`
		TaskStream    struct {
			ID string `json:"id"`
		} `json:"task_stream"`
		Record struct {
			Owner struct {
				SessionID string `json:"session_id"`
				RunID     string `json:"run_id"`
			} `json:"owner"`
		} `json:"record"`
	}
	switch kind {
	case "task_stream_linked":
		if err := json.Unmarshal(event, &e); err != nil {
			return err
		}
		if e.TaskStream.ID != e.TaskID {
			return fmt.Errorf("Muse task stream names another task")
		}
	case "memory_reminder_child_session_linked":
		if err := json.Unmarshal(event, &e); err != nil {
			return err
		}
		if e.ParentSession != stream || e.ParentRun != run || e.TaskStream.ID != e.TaskID {
			return fmt.Errorf("Muse reminder link names another session, run or task")
		}
	case "goal_usage_attribution":
		if err := json.Unmarshal(event, &e); err != nil {
			return err
		}
		if e.Record.Owner.SessionID != stream || e.Record.Owner.RunID != run {
			return fmt.Errorf("Muse usage attribution names another session or run")
		}
	}
	return nil
}

func museJSONObject(raw json.RawMessage) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && len(object) > 0
}

func museTaskEvent(kind string) bool {
	switch kind {
	case "proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed", "failed", "cancelled":
		return true
	}
	return false
}

// A run the fold has seen start, whether still active or terminated.
func museRunKnown(f *museLogFold, id string) bool {
	state := f.RunLifecycle[id]
	return state == museRunActive || state == museRunTerminated
}

// museBindSession resolves the owned session identity before the fold: the
// single distinct stream of the opened records. A log that opens two
// sessions is unknown state; a log with no opened record binds nothing and
// can never be idle.
func museBindSession(lines []*museDecodedRecord) (string, error) {
	bound := ""
	for _, record := range lines {
		if record.Type != "session.opened.observed" || record.Stream.ID == "" {
			continue
		}
		if bound != "" && record.Stream.ID != bound {
			return "", fmt.Errorf("Muse log opens a second session")
		}
		bound = record.Stream.ID
	}
	return bound, nil
}

// Muse 1.4.1 retains permission transactions as one atomic frame whose children
// are serialized records. A malformed frame is unknown, never an empty log.
func museLogRecords(data []byte) ([]*museDecodedRecord, error) {
	var records []*museDecodedRecord
	split := bytes.Split(data, []byte{'\n'})
	for i, line := range split {
		// Only the zero-length terminator after the final newline is not
		// a record. Every other line carries exactly one complete JSON
		// record: no line is skipped, including whitespace-only lines
		// and interior empty lines.
		if i == len(split)-1 && len(line) == 0 {
			continue
		}
		envelope, err := decodeMuseLogRecord(line)
		if err != nil {
			return nil, err
		}
		if envelope.Marker == "omitted_live_only" {
			// A closed task_tool_delta_v1 marker retains the omitted
			// ephemeral status position. Keep it in the identity/sequence
			// fold; it neither starts nor settles any work.
			envelope.Type = "@marker/omitted_live_only"
			envelope.ID, envelope.Sequence = envelope.Position.ID, envelope.Position.Sequence
			envelope.Payload, err = json.Marshal(map[string]any{"omitted_record": envelope.known["omitted_record"]})
			if err != nil {
				return nil, err
			}
		}
		if envelope.Frame == "" {
			if envelope.Type == "" {
				return nil, fmt.Errorf("Muse record lacks payload type")
			}
			records = append(records, envelope)
			continue
		}
		if envelope.Frame != "session_permission_transaction" || envelope.Version != 1 || len(envelope.Children) == 0 {
			return nil, fmt.Errorf("unsupported Muse retained frame")
		}
		// The digest SPELLING is pinned (3/3 captured digests read
		// sha256: plus 64 hex digits); a malformed digest is unknown
		// state. The digest CONTENT is unverified: no derivation
		// from the captured children reproduces it (see the r11
		// Stop-The-Line evidence), so no checksum requirement is
		// invented and real frames still fold.
		if !museFrameDigestShape(envelope.Digest) {
			return nil, fmt.Errorf("Muse retained frame digest is malformed")
		}
		// The captured permission frame carries exactly two
		// children in order (3/3): the format declaration, then
		// the profile commit. Any other child count, index or
		// kind inside the frame is unknown state and is never
		// folded: a run terminal smuggled into a permission
		// frame must not settle a run.
		want := []string{"runtime.session.permission_format_declared", "runtime.session.permission_profile_committed"}
		if len(envelope.Children) != len(want) {
			return nil, fmt.Errorf("Muse retained frame has uncaptured children")
		}
		for i, child := range envelope.Children {
			if child.Index != i {
				return nil, fmt.Errorf("Muse retained frame child order is uncaptured")
			}
			record, err := decodeMuseLogRecord([]byte(child.Record))
			if err != nil {
				return nil, err
			}
			if record.Type != want[i] {
				return nil, fmt.Errorf("Muse retained frame child is uncaptured")
			}
			records = append(records, record)
		}
	}
	return records, nil
}

// museFrameDigestShape pins the captured digest spelling: sha256:
// plus 64 hexadecimal digits. Hex case is not pinned: case is a
// display choice, and refusing an uppercase spelling would lock out a
// real frame over nothing.
func museFrameDigestShape(digest string) bool {
	hex, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(hex) != 64 {
		return false
	}
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
			continue
		}
		return false
	}
	return true
}

func (f *museLogFold) activeRunCount() int {
	n := 0
	for _, state := range f.RunLifecycle {
		if state == museRunActive {
			n++
		}
	}
	return n
}

// Any task with a planning prefix and no terminal is pending work, whether
// or not it reached started and whether or not its parent run has ended:
// run-bound and mirror lifecycles alike.
func (f *museLogFold) pendingTaskCount() int {
	n := 0
	for _, state := range f.Tasks {
		if state != museTaskTerminated {
			n++
		}
	}
	for _, state := range f.Mirror {
		if state != museTaskTerminated {
			n++
		}
	}
	return n
}

func (f *museLogFold) pendingIntentCount() int {
	n := 0
	for _, intent := range f.Intents {
		if intent.State != museIntentMaterialized {
			n++
		}
	}
	return n
}

// A received command stays pending until its matching settle: 8/8
// captured receives settle exactly once, received before settled,
// both spellings alike. Received-but-unsettled intake is pending
// work, not idle (r12). In-flight intake is known-busy, not unknown
// state, so it gates idle() rather than failing the fold.
func (f *museLogFold) pendingIntakeCount() int {
	n := 0
	for id := range f.IntakeReceived {
		if !f.IntakeSettled[id] {
			n++
		}
	}
	return n
}

func (f *museLogFold) idle() bool {
	for _, effect := range f.ToolEffects {
		if !effect.Terminal {
			return false
		}
	}
	// Idle requires an OPEN session epoch (r13): the epoch call below
	// is exactly Opened && !Ended && !ReopenPending.
	return f.epoch() == museEpochOpen && !f.Unknown && f.activeRunCount() == 0 && len(f.Queued) == 0 && len(f.DrainTargetPending) == 0 && f.pendingTaskCount() == 0 && f.pendingIntentCount() == 0 && f.pendingIntakeCount() == 0
}

// Discovery is bound to route_facts of our own child, never the latest session
// or a side MSP host (which sees the TUI session as notLoaded).
func findMuseLog(root string, pid int) (string, error) {
	var found string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "subagent" {
			return filepath.SkipDir
		}
		if d.IsDir() || d.Name() != "session.jsonl" {
			return nil
		}
		// The pinned TUI's log is YYYY/MM/DD/<session>/session.jsonl.
		// Nested subagent logs have a separate grammar and do not identify a
		// hosted TUI. Never fold them as primary-session candidates on resume.
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		if len(strings.Split(rel, string(filepath.Separator))) != 5 {
			return nil
		}
		f, e := readMuseLog(path)
		if e != nil {
			var prefix *MuseStartupPrefixError
			if errors.As(e, &prefix) && museLogNamesPID(path, pid) {
				// A valid route attributes this invalid prefix to our child.
				// Return the actual refusal, never wait for a deadline to
				// manufacture a different verdict.
				found = path
				return e
			}
			// L3G discovery isolation (M54): a session this host
			// cannot fold — a sibling surface's broken or future
			// log — never fails another session's discovery. The
			// walk skips it and keeps looking; it is never
			// folded and never trusted.
			return nil
		}
		if f.PID == pid {
			if found != "" {
				return fmt.Errorf("multiple Muse logs name owned child")
			}
			found = path
		}
		return nil
	})
	if err != nil {
		return found, err
	}
	if found == "" {
		return "", fmt.Errorf("Muse owned session log not found")
	}
	return found, nil
}

// museLogNamesPID attributes a failed prefix only through a fully validated,
// projected route record. It cannot admit a session: the original fold error
// is still returned. Incomplete/malformed routes and additive PID fields are
// never ownership evidence. A later valid route supersedes a historical one.
func museLogNamesPID(path string, pid int) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	bound := int64(0)
	lines := bytes.Split(data, []byte{'\n'})
	for _, line := range lines[:len(lines)-1] {
		record, err := decodeMuseLogRecord(line)
		if err != nil || record.Type != "runtime.session.route_facts" {
			continue
		}
		bound, _ = museStartupPrefixPID(record.known)
	}
	return bound > 0 && bound == int64(pid)
}
func museTextResult(t museRunTerminal) *sm.ProviderRunResult {
	result := &sm.ProviderRunResult{ExitCode: 1, Status: sm.ProviderRunFailed, ProviderStatus: t.Terminal, Output: t.Text, Recoverable: true}
	switch t.Terminal {
	case "completed":
		result.ExitCode = 0
		result.Status = sm.ProviderRunIdle
		result.Recoverable = true
		// Text goals have no native completion bit. A successful turn is idle;
		// board acceptance remains the authority for completion.
		for _, line := range strings.Split(t.Text, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "BLOCKED:") {
				result.Status = sm.ProviderRunBlocked
				result.ProviderStatus = "blocked"
				result.Recoverable = false
				result.Detail = strings.TrimSpace(line)
				break
			}
		}
	case "failed", "cancelled":
		// Echo captures do not establish the M2 error taxonomy. Never infer an
		// auth/model/retry classification from uncaptured fields or output words.
		result.ProviderStatus = "unknown_failure:" + t.Terminal
		result.Detail = "Pinned Echo corpus does not establish this failure outcome"
		result.Recoverable = false

	default:
		result.ProviderStatus = "unknown_terminal:" + t.Terminal
		result.Recoverable = false
	}
	return result
}
