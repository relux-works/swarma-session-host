package musepty

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestMusePTY147FixturesContainNoPrivateOperatorValues(t *testing.T) {
	root := "testdata/muse-1.4.7"
	username := regexp.MustCompile(`(?i)\biv\b`)
	files := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		text := strings.ToLower(path + "\n" + string(data))
		for _, marker := range []string{"/users/", "relux", "agents-infra", "~/.agents", "$home/.agents"} {
			if strings.Contains(text, marker) {
				t.Errorf("%s contains forbidden operator marker %q", path, marker)
			}
		}
		if username.MatchString(text) {
			t.Errorf("%s contains the operator's home-directory username", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("privacy scan found no fixtures")
	}
}

// Abbreviated operator paths can evade the general marker scan. Both META
// stills must carry the exact neutral replacement, including the SGR still.
func TestMusePTY147METAStartupFixturesHaveNeutralPath(t *testing.T) {
	sgr := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	for _, file := range []string{"screen-meta-startup-plain.txt", "screen-meta-startup-sgr.txt"} {
		text := sgr.ReplaceAllString(string(muse147Fixture(t, file)), "")
		footers := 0
		for _, row := range strings.Split(text, "\n") {
			if !strings.HasPrefix(row, "  muse-") {
				continue
			}
			footers++
			parts := strings.Split(row, " · ")
			if len(parts) != 4 || parts[2] != "/h/f/ws" {
				t.Errorf("%s contains an unsanitized or malformed footer path", file)
			}
		}
		if footers != 1 {
			t.Errorf("%s footer count=%d, want 1", file, footers)
		}
	}
}

func TestMusePTY147SanitizedSessionLogAdmission(t *testing.T) {
	primary, subagents := 0, 0
	err := filepath.WalkDir("testdata/muse-1.4.7", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != "session.jsonl" {
			return nil
		}
		t.Run(path, func(t *testing.T) {
			_, err := readMuseLog(path)
			if strings.Contains(filepath.ToSlash(path), "/subagent/") {
				subagents++
				// These discovery-depth witnesses have no opened session and
				// contain an unsupported fatal-error classification. Sanitizing
				// their values must retain the host's existing refusal.
				var shape *museRecordShapeError
				if !errors.As(err, &shape) || shape.Key != "runtime.session/run/run_fatal_error_classified" || shape.Code != MuseStartupPrefixUnknownRecord {
					t.Fatalf("subagent admission changed: %v", err)
				}
				return
			}
			primary++
			if err != nil {
				t.Fatal(err)
			}
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if primary != 2 || subagents != 2 {
		t.Fatalf("checked %d primary and %d subagent logs, want two each", primary, subagents)
	}
}
