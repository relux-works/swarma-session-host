package musebuilds

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// Reservation outcomes. A reservation counts as the day's freeze
// attempt in every outcome, including failed: it is never silently
// refunded, and an incomplete copy is never recopied under the spent
// slot.
const (
	outcomePending   = "pending"
	outcomePublished = "published"
	outcomeFailed    = "failed"
)

const (
	reservationSchema   = 1
	reservationMaxBytes = 64 * 1024
)

// Reservation is one durable day reservation: host, user, exact
// build, full digest, transaction, UTC day and outcome. The file name
// keys the lifetime build and digest; the content repeats them so a
// mismatching file refuses instead of silently serving another key.
type Reservation struct {
	SchemaVersion int    `json:"schema"`
	Host          string `json:"host"`
	User          string `json:"user"`
	Build         string `json:"build"`
	SHA256        string `json:"sha256"`
	Transaction   string `json:"transaction"`
	Day           string `json:"day"`
	ReservedAt    string `json:"reserved_at"`
	Outcome       string `json:"outcome"`
	OutcomeDetail string `json:"outcome_detail"`
	OutcomeAt     string `json:"outcome_at"`
}

// The UTC day is accounting data, never part of lifetime tuple identity.
// Keep the day parameter at internal fixture call sites to make that explicit.
func reservationFileName(_ string, build, sha256 string) string {
	return build + "_" + sha256 + ".json"
}

func parseReservationName(name string) (day, build, sha256 string, err error) {
	rest, ok := strings.CutSuffix(name, ".json")
	if !ok || len(rest) < 66 || rest[len(rest)-65] != '_' {
		return "", "", "", fmt.Errorf("%q is not a ledger reservation", name)
	}
	build, sha256 = rest[:len(rest)-65], rest[len(rest)-64:]
	if _, err := muse.ParseBuildID(build); err != nil {
		return "", "", "", fmt.Errorf("%q carries no build identity", name)
	}
	if !isLowerHex64(sha256) {
		return "", "", "", fmt.Errorf("%q carries no full digest", name)
	}
	return "", build, sha256, nil
}

func validateReservation(r Reservation, day, build, sha256 string) error {
	if r.SchemaVersion != reservationSchema {
		return fmt.Errorf("schema is %d, want %d", r.SchemaVersion, reservationSchema)
	}
	if r.Host == "" || r.User == "" {
		return fmt.Errorf("reservation names no host or user")
	}
	if r.Transaction == "" {
		return fmt.Errorf("reservation carries no transaction")
	}
	if r.Build != build || r.SHA256 != sha256 || r.Day != day {
		return fmt.Errorf("reservation content disagrees with its file name")
	}
	if _, err := time.Parse("2006-01-02", r.Day); err != nil {
		return fmt.Errorf("reservation day %q does not parse", r.Day)
	}
	reservedAt, err := time.Parse(time.RFC3339, r.ReservedAt)
	if err != nil {
		return fmt.Errorf("reserved_at %q does not parse: %w", r.ReservedAt, err)
	}
	if reservedAt.UTC().Format("2006-01-02") != r.Day {
		return fmt.Errorf("reserved_at %q is not on day %s", r.ReservedAt, r.Day)
	}
	switch r.Outcome {
	case outcomePending, outcomePublished, outcomeFailed:
	default:
		return fmt.Errorf("outcome %q is not pending, published or failed", r.Outcome)
	}
	if r.OutcomeAt != "" {
		if _, err := time.Parse(time.RFC3339, r.OutcomeAt); err != nil {
			return fmt.Errorf("outcome_at %q does not parse: %w", r.OutcomeAt, err)
		}
	}
	return nil
}

func readReservation(ctx context.Context, path string) (Reservation, error) {
	dir, err := openDirPath(ctx, filepath.Dir(path), false)
	if err != nil {
		return Reservation{}, err
	}
	defer dir.close()
	return readReservationAt(ctx, dir, filepath.Base(path))
}

func readReservationAt(ctx context.Context, dir *pinnedDir, name string) (Reservation, error) {
	_, build, sha256, err := parseReservationName(name)
	if err != nil {
		return Reservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return Reservation{}, err
	}
	file, err := dir.openChildFile(name)
	if err != nil {
		return Reservation{}, err
	}
	defer file.Close()
	raw, _, _, err := readMetadataFD(ctx, file, reservationMaxBytes, true)
	if err != nil {
		return Reservation{}, err
	}
	var reservation Reservation
	if err := decodeStrictJSON(raw, reservationMaxBytes, &reservation); err != nil {
		return Reservation{}, err
	}
	if err := validateReservation(reservation, reservation.Day, build, sha256); err != nil {
		return Reservation{}, err
	}
	return reservation, nil
}

// O_EXCL is lifetime tuple uniqueness, including after crashes and rollover.
// Failed/partial reservations are retained and refuse; they are never refunded.
func createReservation(ctx context.Context, dir string, r Reservation) (created bool, winner Reservation, err error) {
	pin, err := openDirPath(ctx, dir, false)
	if err != nil {
		return false, Reservation{}, err
	}
	defer pin.close()
	name := reservationFileName(r.Day, r.Build, r.SHA256)
	raw, err := json.Marshal(r)
	if err != nil {
		return false, Reservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return false, Reservation{}, err
	}
	file, err := pin.openFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			winner, readErr := readReservationAt(ctx, pin, name)
			return false, winner, readErr
		}
		return false, Reservation{}, err
	}
	defer file.Close()
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return false, Reservation{}, err
	}
	if err := fsyncFile(file); err != nil {
		return false, Reservation{}, err
	}
	if err := file.Close(); err != nil {
		return false, Reservation{}, err
	}
	if err := pin.sync(); err != nil {
		return false, Reservation{}, fmt.Errorf("%w: %v", errPublicationUnknown, err)
	}
	return true, r, nil
}

// Failure receipts have their own short cleanup budget so a cancelled copy
// can retain its observed outcome; no executable-byte operation runs here.
func recordReservationOutcome(cfg HostConfig, r Reservation, outcome, detail string) error {
	ctx, cancel := context.WithTimeout(context.Background(), stateLockWaitCap)
	defer cancel()
	dir, err := openDirPath(ctx, cfg.LedgerDir, false)
	if err != nil {
		return err
	}
	defer dir.close()
	r.Outcome, r.OutcomeDetail = outcome, detail
	r.OutcomeAt = cfg.clock()().UTC().Format(time.RFC3339)
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return atomicDocumentAt(ctx, dir, reservationFileName(r.Day, r.Build, r.SHA256), ".reservation-", append(raw, '\n'))
}

// inventoryLedger reads the whole ledger under the global lock: every
// file must parse and validate, or the day guard refuses instead of
// resetting to empty. It returns today's reservation count across
// all outcomes — the guard is host-wide — plus the lifetime reservation for
// this exact build when one exists. Every record must name this
// host; a foreign-host record refuses instead of counting. A record day
// later than today means a backward clock and refuses.
func inventoryLedger(ctx context.Context, dir, today, build, sha256, host string) (int, *Reservation, error) {
	pin, err := openDirPath(ctx, dir, false)
	if err != nil {
		return 0, nil, fmt.Errorf("ledger unreadable: %w", err)
	}
	defer pin.close()
	entries, err := pin.entries()
	if err != nil {
		return 0, nil, fmt.Errorf("ledger unreadable: %w", err)
	}
	count := 0
	var existing *Reservation
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			return 0, nil, fmt.Errorf("ledger holds directory %q", name)
		}
		_, entryBuild, entrySHA, err := parseReservationName(name)
		if err != nil {
			return 0, nil, err
		}
		reservation, err := readReservationAt(ctx, pin, name)
		if err != nil {
			return 0, nil, fmt.Errorf("ledger file %q: %w", name, err)
		}
		if reservation.Host != host {
			return 0, nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}
		day := reservation.Day
		if day > today {
			return 0, nil, fmt.Errorf("ledger holds %q past today %s: the clock moved backward", name, today)
		}
		if day == today {
			count++
		}
		if entryBuild == build && entrySHA == sha256 {
			found := reservation
			existing = &found
		}
	}
	return count, existing, nil
}

// findAdoptionReservation locates this build's reservation on any UTC
// day for crash adoption, newest first. The whole ledger still must
// validate — a corrupt ledger never yields a trustworthy transaction
// — but the day guard does not apply: adoption bills nothing new.
// Every record must name this host; a foreign-host record refuses
// instead of authorizing adoption.
func findAdoptionReservation(ctx context.Context, dir, build, sha256, host string) (*Reservation, error) {
	pin, err := openDirPath(ctx, dir, false)
	if err != nil {
		return nil, fmt.Errorf("ledger unreadable: %w", err)
	}
	defer pin.close()
	entries, err := pin.entries()
	if err != nil {
		return nil, fmt.Errorf("ledger unreadable: %w", err)
	}
	var best *Reservation
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			return nil, fmt.Errorf("ledger holds directory %q", name)
		}
		_, entryBuild, entrySHA, err := parseReservationName(name)
		if err != nil {
			return nil, err
		}
		reservation, err := readReservationAt(ctx, pin, name)
		if err != nil {
			return nil, fmt.Errorf("ledger file %q: %w", name, err)
		}
		if reservation.Host != host {
			return nil, fmt.Errorf("ledger file %q names host %q, want %q", name, reservation.Host, host)
		}
		if entryBuild != build || entrySHA != sha256 {
			continue
		}
		if best == nil {
			found := reservation
			best = &found
		}
	}
	return best, nil
}
