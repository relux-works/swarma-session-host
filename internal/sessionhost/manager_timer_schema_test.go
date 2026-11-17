package sessionhost

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func managerTimerRecordErrorMatches(want string, got error) bool {
	if got == nil {
		return want == "schema_accept"
	}
	return got.Error() == want
}

// Independently fixed record bytes/verdicts exercise the closed data language,
// canonical hashes and intrinsic relationships; they establish no live issuer,
// eligibility, effect, durable state transition or native-provider capability.
func TestManagerTimerFrozenRecordLanguage(t *testing.T) {
	data, err := os.ReadFile("testdata/manager_timer_records.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Records []struct {
			ID        string `json:"id"`
			Type      string `json:"record_type"`
			Input     string `json:"input_json"`
			Expected  string `json:"expected"`
			Canonical string `json:"canonical_utf8"`
			Digest    string `json:"domain_digest"`
		} `json:"record_vectors"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Records) != 63 {
		t.Fatalf("frozen record cases = %d", len(fixture.Records))
	}
	var retained map[string]any
	for _, v := range fixture.Records {
		if v.ID == "ProviderCall-reserve-valid" {
			retained, err = decodeManagerTimerObject([]byte(v.Input + "\n"))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, v := range fixture.Records {
		t.Run(v.ID, func(t *testing.T) {
			o, got := decodeManagerTimerObject([]byte(v.Input + "\n"))
			if got == nil {
				got = validateManagerTimerRecord(v.Type, o)
			}
			if got == nil && v.ID == "ProviderCall-changed-retained-submission" {
				got = managerTimerSameIntent(o, retained)
			}
			if v.Expected != "schema_accept" {
				if !managerTimerRecordErrorMatches(v.Expected, got) {
					t.Fatalf("verdict = %v, want %s", got, v.Expected)
				}
				return
			}
			if got != nil {
				t.Fatal(got)
			}
			canonical, err := canonicalManagerTimerValue(o)
			if err != nil || string(canonical) != v.Canonical {
				t.Fatalf("canonical mismatch: %v", err)
			}
			digest, err := managerTimerDomainDigest(v.Type, o)
			if err != nil || hex.EncodeToString(digest[:]) != v.Digest {
				t.Fatalf("digest mismatch: %v", err)
			}
		})
	}
}

// The verdict checker detects a known correlation bypass in both the direct
// Readback and its nested OwnerResponse. Correct positives remain accepted;
// a setup failure or a refuser cannot count as detecting this plant.
func TestManagerTimerReadbackControlPlant(t *testing.T) {
	data, err := os.ReadFile("testdata/manager_timer_records.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Records []struct {
			ID       string `json:"id"`
			Type     string `json:"record_type"`
			Input    string `json:"input_json"`
			Expected string `json:"expected"`
		} `json:"record_vectors"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	detected, positive := 0, 0
	for _, v := range fixtures.Records {
		if v.ID != "Readback-valid" && v.ID != "OwnerResponse-valid" && v.ID != "Readback-input-owner-mismatch" && v.ID != "OwnerResponse-input-owner-mismatch" {
			continue
		}
		o, err := decodeManagerTimerObject([]byte(v.Input + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		baseline := validateManagerTimerRecord(v.Type, o)
		if !managerTimerRecordErrorMatches(v.Expected, baseline) {
			t.Fatalf("baseline %s: %v", v.ID, baseline)
		}
		planted := baseline
		if baseline == managerTimerBindingMismatch {
			planted = nil
		} // Deliberately omit only the correlation refusal.
		if v.Expected == "schema_accept" {
			if !managerTimerRecordErrorMatches(v.Expected, planted) {
				t.Fatalf("plant broke positive %s", v.ID)
			}
			positive++
		} else if !managerTimerRecordErrorMatches(v.Expected, planted) {
			detected++
		}
	}
	if detected != 2 || positive != 2 {
		t.Fatalf("plant detections=%d positive controls=%d", detected, positive)
	}
}

// Each scalar has valid boundary neighbors and a forbidden form. A well-shaped
// record still needs authority/context checks before it can cause any effect.
func TestManagerTimerScalarBounds(t *testing.T) {
	for _, c := range []struct {
		kind, input string
		valid       bool
	}{
		{"U", "9007199254740991", true}, {"U", "9007199254740992", false},
		{"N", "9223372036854775807", true}, {"N", "9223372036854775808", false},
		{"U+", "0", false}, {"U+", "1", true},
		{"PATH", `"/fixture/a"`, true}, {"PATH", `"/fixture/../a"`, false},
		{"PATH", `"/fixture/a/"`, false}, {"PATH", `"relative"`, false},
		{"T", `"2026-11-17T19:00:00Z"`, true}, {"T", `"2026-02-30T19:00:00Z"`, false},
		{"T", `"2026-11-17T19:00:00.0Z"`, false},
		{"UUID", `"00000000-0000-4000-8000-000000000001"`, true},
		{"UUID", `"00000000-0000-4000-8000-00000000000A"`, false},
		{"ID", `"fixture_1-2"`, true}, {"ID", `"-fixture"`, false},
		{"S", `"α"`, true}, {"S", `"a\n"`, false},
	} {
		t.Run(c.kind+":"+c.input, func(t *testing.T) {
			o, err := decodeManagerTimerObject([]byte(`{"value":` + c.input + "}\n"))
			if err == nil {
				err = managerTimerField(c.kind, o["value"])
			}
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v: %v", c.valid, err)
			}
		})
	}
}

// Programmatically supplied json.Number values cannot smuggle non-JSON number
// spellings past the decoder; valid exact integers and -0 still canonicalize.
func TestManagerTimerDirectNumberGrammar(t *testing.T) {
	for _, text := range []string{"+1", "01", " 1", "1e0", "1.0"} {
		if _, ok := managerTimerInteger(json.Number(text)); ok {
			t.Fatalf("accepted noninteger spelling %q", text)
		}
		if _, err := canonicalManagerTimerValue(json.Number(text)); err != managerTimerInvalid {
			t.Fatalf("canonical %q=%v", text, err)
		}
	}
	for _, text := range []string{"0", "-0", "1", "9007199254740993", "9223372036854775807"} {
		if _, ok := managerTimerInteger(json.Number(text)); !ok {
			t.Fatalf("rejected valid integer %q", text)
		}
		if _, err := canonicalManagerTimerValue(json.Number(text)); err != nil {
			t.Fatal(err)
		}
	}
}
