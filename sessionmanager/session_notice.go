package sessionmanager

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SessionNoticeContract versions the on-disk notice envelope. A notice whose
// contract this build does not understand is left in the inbox rather than
// delivered as something it is not.
const SessionNoticeContract = "task-board-session-notice-v1"

// sessionNoticeBurstLimit bounds how many entries are spelled out IN FULL —
// element, role, runtime and summary. A fan-out of fifty runs must not paste a
// wall of text into the session.
//
// It does not bound which runs are named. The pump deletes every notice it
// reads, so this message is the only channel that says a given run finished; an
// id dropped here is an observation the orchestrator never gets back. Beyond
// the limit each remaining run still contributes its id and terminal state,
// which is short, and only the per-run detail is dropped.
const sessionNoticeBurstLimit = 8

// SessionNotice is one terminal-state observation addressed at the manager
// session that spawned the run.
//
// SessionID is the addressing decision and is made by the producer from its own
// environment: a run spawned by a different session, or by a process that is not
// a manager-hosted session at all, never produces a notice for this session.
type SessionNotice struct {
	Contract    string    `json:"contract"`
	SessionID   string    `json:"session_id"`
	RunID       string    `json:"run_id"`
	ElementID   string    `json:"element_id,omitempty"`
	Role        string    `json:"role,omitempty"`
	Runtime     string    `json:"runtime,omitempty"`
	State       string    `json:"state"`
	Summary     string    `json:"summary,omitempty"`
	NextCommand string    `json:"next_command,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// sessionNoticeGoalAckMarkers are the rendered acknowledgements the Claude goal
// waiters match on. Injected text is echoed by the TUI into the same output
// stream the scanner reads, so a notice carrying one of these phrases could
// resolve or corrupt an armed goal acknowledgement. They are refused at the
// injection boundary and neutralized when a notice is composed.
//
// The list is deliberately the union of every marker any goal waiter arms:
// claudeGoalAckConditionedMarkers (goal set/updated/active, plus the bare
// "goal:" overlay), the clear/status waiters ("goal cleared", "no goal set",
// "goal active:"), the owner-run evaluator ("goal achieved", "goal could not be
// achieved"), and the shared failure marker ("unknown command").
var sessionNoticeGoalAckMarkers = []string{
	"goal set:",
	"goal updated:",
	"goal active:",
	"goal cleared",
	"no goal set",
	"goal achieved",
	"goal could not be achieved",
	"unknown command",
	"goal:",
}

// ValidateSessionNoticeText is the fail-closed gate on everything typed into a
// managed session by the notice plane.
//
// It is not a formatting nicety. The notice write path shares one PTY with the
// native goal plane, and the scanner that resolves a goal acknowledgement reads
// the provider's echo of whatever was typed. A notice that contained "Goal
// set:" would be indistinguishable from the provider acknowledging a revision
// it never received, so such text is refused before a single byte is written.
func ValidateSessionNoticeText(text string) error {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return fmt.Errorf("session notice text is required")
	}
	lowered := strings.ToLower(trimmed)
	for _, marker := range sessionNoticeGoalAckMarkers {
		if strings.Contains(lowered, marker) {
			return fmt.Errorf(
				"session notice text carries the native goal acknowledgement marker %q and was refused",
				marker,
			)
		}
	}
	return nil
}

// sanitizeSessionNoticeField neutralizes goal acknowledgement markers inside
// operator-supplied text (a task summary, a failure message) so a real notice
// is deliverable instead of refused. The gate above still runs on the composed
// result: sanitizing is the recovery, not the check.
//
// Two substitutions cover the whole marker set by construction, which is why
// this is not a per-marker rewrite: every marker except one contains "goal", so
// breaking that word removes all of them at once, and "unknown command" is
// broken on its own. A per-marker rewrite would leave the recombination hole
// where deleting one marker's colon spells another marker exactly.
func sanitizeSessionNoticeField(value string) string {
	collapsed := strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if collapsed == "" {
		return ""
	}
	collapsed = replaceFold(collapsed, "goal", "g0al")
	collapsed = replaceFold(collapsed, "unknown command", "unknown-command")
	return collapsed
}

// replaceFold replaces every case-insensitive occurrence of old with new.
func replaceFold(value, old, replacement string) string {
	if old == "" {
		return value
	}
	var b strings.Builder
	loweredOld := strings.ToLower(old)
	for {
		index := strings.Index(strings.ToLower(value), loweredOld)
		if index < 0 {
			b.WriteString(value)
			return b.String()
		}
		b.WriteString(value[:index])
		b.WriteString(replacement)
		value = value[index+len(old):]
	}
}

// FormatSessionNoticeBatch renders one coalesced burst as the single line typed
// into the session. The composed text is guaranteed to pass
// ValidateSessionNoticeText for any input, because every free-form field is
// sanitized first.
func FormatSessionNoticeBatch(notices []SessionNotice) string {
	if len(notices) == 0 {
		return ""
	}
	entries := make([]string, 0, len(notices))
	shown := notices
	if len(shown) > sessionNoticeBurstLimit {
		shown = shown[:sessionNoticeBurstLimit]
	}
	for _, notice := range shown {
		entries = append(entries, formatSessionNoticeEntry(notice))
	}
	var b strings.Builder
	if len(notices) == 1 {
		b.WriteString("task-board run ")
	} else {
		fmt.Fprintf(&b, "task-board runs (%d reached a terminal state) ", len(notices))
	}
	b.WriteString(strings.Join(entries, " | "))
	if rest := notices[len(shown):]; len(rest) > 0 {
		brief := make([]string, 0, len(rest))
		for _, notice := range rest {
			brief = append(brief, fmt.Sprintf(
				"%s %s",
				sanitizeSessionNoticeField(notice.RunID),
				sanitizeSessionNoticeField(notice.State),
			))
		}
		fmt.Fprintf(&b, " | +%d more: %s", len(rest), strings.Join(brief, ", "))
	}
	if next := strings.TrimSpace(notices[0].NextCommand); next != "" {
		fmt.Fprintf(&b, "; next: %s", sanitizeSessionNoticeField(next))
	}
	return b.String()
}

func formatSessionNoticeEntry(notice SessionNotice) string {
	var b strings.Builder
	b.WriteString(sanitizeSessionNoticeField(notice.RunID))
	fmt.Fprintf(&b, " %s", sanitizeSessionNoticeField(notice.State))
	if element := sanitizeSessionNoticeField(notice.ElementID); element != "" {
		fmt.Fprintf(&b, " for %s", element)
	}
	role := sanitizeSessionNoticeField(notice.Role)
	runtime := sanitizeSessionNoticeField(notice.Runtime)
	switch {
	case role != "" && runtime != "":
		fmt.Fprintf(&b, " (%s/%s)", role, runtime)
	case role != "":
		fmt.Fprintf(&b, " (%s)", role)
	case runtime != "":
		fmt.Fprintf(&b, " (%s)", runtime)
	}
	if summary := sanitizeSessionNoticeField(notice.Summary); summary != "" {
		fmt.Fprintf(&b, ": %s", truncateSessionNoticeSummary(summary))
	}
	return b.String()
}

const sessionNoticeSummaryLimit = 160

func truncateSessionNoticeSummary(summary string) string {
	if len(summary) <= sessionNoticeSummaryLimit {
		return summary
	}
	return strings.TrimSpace(summary[:sessionNoticeSummaryLimit]) + "..."
}

// sessionNoticeInbox is the durable per-session queue. Every notice is one
// atomically renamed file, so concurrent producers (one runner process per
// terminal run) never need a lock and a crash mid-write cannot corrupt the
// queue.
//
// Durability is what makes the detached case correct: a notice for a session
// with no attached terminal is not dropped and not injected into a session
// nobody is reading; it stays here until the next attach drains it.
type sessionNoticeInbox struct {
	dir string
}

func newSessionNoticeInbox(dir string) *sessionNoticeInbox {
	return &sessionNoticeInbox{dir: dir}
}

// validateSessionNoticeSessionID keeps an addressed session id usable as one
// path segment. The producer is an ordinary board process, so the value reaching
// this store is not trusted to be a manager-minted handle.
func validateSessionNoticeSessionID(sessionID string) (string, error) {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return "", fmt.Errorf("session notice requires a session id")
	}
	if len(trimmed) > 128 {
		return "", fmt.Errorf("session notice session id is too long")
	}
	for _, r := range trimmed {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return "", fmt.Errorf("session notice session id %q contains unsupported characters", sessionID)
		}
	}
	return trimmed, nil
}

func (i *sessionNoticeInbox) sessionDir(sessionID string) (string, error) {
	validated, err := validateSessionNoticeSessionID(sessionID)
	if err != nil {
		return "", err
	}
	return filepath.Join(i.dir, validated), nil
}

func (i *sessionNoticeInbox) enqueue(notice SessionNotice) error {
	dir, err := i.sessionDir(notice.SessionID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(notice.RunID) == "" {
		return fmt.Errorf("session notice requires a run id")
	}
	if strings.TrimSpace(notice.State) == "" {
		return fmt.Errorf("session notice requires a terminal state")
	}
	notice.Contract = SessionNoticeContract
	if notice.CreatedAt.IsZero() {
		notice.CreatedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating session notice inbox %s: %w", dir, err)
	}
	payload, err := json.Marshal(notice)
	if err != nil {
		return fmt.Errorf("encoding session notice: %w", err)
	}
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Errorf("generating session notice identity: %w", err)
	}
	name := fmt.Sprintf("%020d-%s.json", notice.CreatedAt.UTC().UnixNano(), hex.EncodeToString(suffix))
	temp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(temp, append(payload, '\n'), 0o600); err != nil {
		return fmt.Errorf("writing session notice: %w", err)
	}
	if err := os.Rename(temp, filepath.Join(dir, name)); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("publishing session notice: %w", err)
	}
	return nil
}

// pending returns every readable notice for one session in creation order,
// together with the paths that back them.
//
// An unreadable or malformed entry is reported as an error and its path is NOT
// returned for deletion: a failed read is not an absence, and silently dropping
// it would turn a corrupted queue into an empty one.
func (i *sessionNoticeInbox) pending(sessionID string) ([]SessionNotice, []string, error) {
	dir, err := i.sessionDir(sessionID)
	if err != nil {
		return nil, nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("reading session notice inbox %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	var (
		notices []SessionNotice
		paths   []string
		failure error
	)
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			failure = fmt.Errorf("reading session notice %s: %w", path, readErr)
			continue
		}
		var notice SessionNotice
		if decodeErr := json.Unmarshal(data, &notice); decodeErr != nil {
			failure = fmt.Errorf("decoding session notice %s: %w", path, decodeErr)
			continue
		}
		if notice.Contract != SessionNoticeContract {
			failure = fmt.Errorf("session notice %s carries unsupported contract %q", path, notice.Contract)
			continue
		}
		notices = append(notices, notice)
		paths = append(paths, path)
	}
	return notices, paths, failure
}

func (i *sessionNoticeInbox) remove(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

// EnqueueSessionNotice is the producer-facing entry point. It is called from the
// tracked-spawn terminal observation hook in the process that spawned the run,
// which is the only process that can prove which manager session owns it.
func EnqueueSessionNotice(layout Layout, notice SessionNotice) error {
	if strings.TrimSpace(layout.NoticesDir) == "" {
		return fmt.Errorf("session manager layout has no notice inbox")
	}
	return newSessionNoticeInbox(layout.NoticesDir).enqueue(notice)
}
