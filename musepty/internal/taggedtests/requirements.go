package taggedtests

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// A requirement records a bounded coverage gap, never a successful kill.
type Requirement struct {
	Test               string `json:"test"`
	Mutant             string `json:"mutant"`
	Killer             string `json:"killer"`
	Env                string `json:"env"`
	DeclaredPresentEnv string `json:"declared_present_env"`
	Host               string `json:"host"`
	Owner              string `json:"owner"`
	Cadence            string `json:"cadence"`
	Created            string `json:"created"`
	FirstCreated       string `json:"first_created"`
	Renewals           int    `json:"renewals"`
	Due                string `json:"due"`
	SHA256             string `json:"sha256,omitempty"`
	Task               string `json:"task"`
	Status             string `json:"status"`
}

// This first occurrence is pinned in source: editing only the manifest cannot
// silently restart the gap's clock. One visible renewal has a 14-day horizon.
const MuseEchoGapFirstCreated = "2026-10-07"

func ValidateRequirements(entries []Entry, now time.Time) error {
	today, _ := time.Parse("2006-01-02", now.UTC().Format("2006-01-02"))
	for _, e := range entries {
		seen := map[string]bool{}
		for _, r := range e.Requires {
			hasTest := false
			for _, test := range e.Tests {
				if test == r.Test {
					hasTest = true
				}
			}
			created, a := time.Parse("2006-01-02", r.Created)
			due, b := time.Parse("2006-01-02", r.Due)
			first, c := time.Parse("2006-01-02", r.FirstCreated)
			digest, digestErr := hex.DecodeString(r.SHA256)
			if !hasTest || r.Mutant == "" || r.Killer == "" || r.Env == "" || r.DeclaredPresentEnv == "" || r.Env == r.DeclaredPresentEnv || strings.TrimSpace(r.Host) == "" || strings.TrimSpace(r.Owner) == "" || r.Cadence != "daily" || !strings.HasPrefix(r.Task, "TASK-") || r.Status != "UNRUN-NEEDS-BINARY" || a != nil || b != nil || c != nil || created.After(today) || created.Before(first) || due.Before(created) || due.After(created.AddDate(0, 0, 7)) || due.After(first.AddDate(0, 0, 14)) || r.Renewals < 0 || r.Renewals > 1 || (r.Renewals == 0 && !created.Equal(first)) || (r.Renewals == 1 && !created.After(first)) || (r.SHA256 != "" && (digestErr != nil || len(digest) != sha256.Size)) || (os.Getenv(r.Env) != "" && r.SHA256 == "") || seen[r.Mutant] {
				return fmt.Errorf("invalid binary requirement for %s/%s/%s: host, owner, daily cadence, immutable first_created, <=7-day due/14-day horizon and pinned sha256 when supplied are required", e.Module, e.Package, r.Mutant)
			}
			if r.Mutant == "live-permissions-modal-only" && r.Killer == "TestMusePTYRealPermissionsModalNeverPastes" && r.FirstCreated != MuseEchoGapFirstCreated {
				return fmt.Errorf("binary requirement first_created cannot be reset for %s", r.Mutant)
			}
			seen[r.Mutant] = true
		}
	}
	return nil
}

// Expiry is a trunk execution failure only for an unresolved binary gap,
// not a restored pinned binary or a blanket per-PR schema failure.
func CheckExpiry(entries []Entry, now time.Time) error {
	today, _ := time.Parse("2006-01-02", now.UTC().Format("2006-01-02"))
	for _, e := range entries {
		for _, r := range e.Requires {
			due, err := time.Parse("2006-01-02", r.Due)
			if err == nil && today.After(due) {
				gap, gapErr := r.Gap(os.Getenv(r.Env), os.Getenv(r.DeclaredPresentEnv))
				if gapErr != nil {
					return gapErr
				}
				if gap == "" {
					continue
				}
			}
			if err != nil || today.After(due) {
				return fmt.Errorf("expired binary gap %s: due=%s task=%s; trunk lane must fail and report P1", r.Mutant, r.Due, r.Task)
			}
		}
	}
	return nil
}

func RequirementFor(entries []Entry, module, pkg, test, mutant, killer string) (Requirement, error) {
	for _, e := range entries {
		if e.Module == module && e.Package == pkg {
			for _, r := range e.Requires {
				if r.Test == test && r.Mutant == mutant && r.Killer == killer {
					return r, nil
				}
			}
		}
	}
	return Requirement{}, fmt.Errorf("binary requirement missing for %s/%s/%s/%s", module, pkg, test, mutant)
}

// An explicit declaration or a supplied binary keeps missing/broken inputs red.
// Undeclared absence is the only permitted UNRUN result; the killer still has
// to establish a behavioral kill when the binary is supplied.
func (r Requirement) Gap(binary, declared string) (string, error) {
	declared = strings.TrimSpace(declared)
	if declared != "" && declared != "0" && declared != "1" {
		return "", fmt.Errorf("%s must be 0 or 1", r.DeclaredPresentEnv)
	}
	if binary != "" {
		info, err := os.Stat(binary)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			return "", fmt.Errorf("declared binary %s unavailable or not executable", r.Env)
		}
		pin, err := hex.DecodeString(r.SHA256)
		if err != nil || len(pin) != sha256.Size {
			return "", fmt.Errorf("declared binary %s requires a pinned sha256", r.Env)
		}
		file, err := os.Open(binary)
		if err != nil {
			return "", err
		}
		h := sha256.New()
		_, hashErr := io.Copy(h, file)
		closeErr := file.Close()
		if hashErr != nil {
			return "", hashErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		if !bytes.Equal(pin, h.Sum(nil)) {
			return "", fmt.Errorf("declared binary %s sha256 mismatch", r.Env)
		}
		return "", nil
	}
	if declared == "1" {
		return "", fmt.Errorf("declared-present binary %s not supplied", r.Env)
	}
	return fmt.Sprintf("UNRUN-NEEDS-BINARY mutant=%s killer=%s requires=%s host=%s owner=%s cadence=%s due=%s task=%s", r.Mutant, r.Killer, r.Env, r.Host, r.Owner, r.Cadence, r.Due, r.Task), nil
}

// A setup failure, skipped test or wrong named test cannot be a behavioral kill.
// The mutant callback is unreachable until the same unmutated killer passes.
type MutationResult struct {
	Output   []byte
	Err      error
	ExitCode int
}

func AfterPassingBaseline(test string, baseline, mutant func() MutationResult) (MutationResult, error) {
	b := baseline()
	if b.Err != nil || b.ExitCode != 0 {
		return MutationResult{}, fmt.Errorf("unmutated named killer %s baseline did not PASS: %v\n%s", test, b.Err, b.Output)
	}
	d := json.NewDecoder(bytes.NewReader(b.Output))
	passed := false
	for {
		var event struct{ Action, Test string }
		err := d.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			return MutationResult{}, fmt.Errorf("unmutated named killer %s baseline did not PASS: invalid JSON: %w", test, err)
		}
		if event.Test == test {
			if event.Action == "skip" || event.Action == "fail" {
				return MutationResult{}, fmt.Errorf("unmutated named killer %s baseline did not PASS: %s", test, event.Action)
			}
			if event.Action == "pass" {
				passed = true
			}
		}
	}
	if !passed {
		return MutationResult{}, fmt.Errorf("unmutated named killer %s baseline did not PASS: no named pass event", test)
	}
	return mutant(), nil
}

// Regeneration must preserve gap metadata, and refuse orphaned requirements.
func PreserveRequirements(derived, pin []Entry) ([]Entry, error) {
	for _, e := range pin {
		for _, r := range e.Requires {
			attached := false
			for i := range derived {
				if derived[i].Module == e.Module && derived[i].Package == e.Package {
					for _, test := range derived[i].Tests {
						if test == r.Test {
							derived[i].Requires = append(derived[i].Requires, r)
							attached = true
						}
					}
				}
			}
			if !attached {
				return nil, fmt.Errorf("orphaned binary requirement %s/%s/%s", e.Module, e.Package, r.Test)
			}
		}
	}
	return derived, nil
}

func reportRequirements(pin []Entry) error {
	return reportRequirementsAt(pin, time.Now().UTC())
}

func reportRequirementsAt(pin []Entry, now time.Time) error {
	var lines []string
	var failures []string
	if err := CheckExpiry(pin, now); err != nil {
		failures = append(failures, err.Error())
		lines = append(lines, "FAIL: "+err.Error())
	}
	for _, e := range pin {
		for _, r := range e.Requires {
			gap, err := r.Gap(os.Getenv(r.Env), os.Getenv(r.DeclaredPresentEnv))
			if err != nil {
				failures = append(failures, err.Error())
				lines = append(lines, "FAIL: "+err.Error())
			} else if gap != "" {
				lines = append(lines, gap)
			}
		}
	}
	if len(lines) > 0 {
		body := "## Tagged mutation coverage gaps\n\n" + strings.Join(lines, "\n\n") + "\n"
		fmt.Print(body)
		if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, writeErr := f.WriteString(body)
			closeErr := f.Close()
			if writeErr != nil {
				return writeErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return nil
}
