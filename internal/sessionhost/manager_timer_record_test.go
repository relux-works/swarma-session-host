package sessionhost

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// The codec preserves exact integer/Unicode data and canonical bytes. These
// fixtures establish no manager authority, provider semantics or native effect.
func TestManagerTimerCanonicalNumbersAndUnicode(t *testing.T) {
	frame := []byte("{\n \"trusted\":true,\"observed_at\":\"2026-11-17T17:00:00Z\",\"monotonic_ns\":9007199254740993,\"boot_id\":\"fixture-boot\"\n}\n")
	object, err := decodeManagerTimerObject(frame)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalManagerTimerValue(object)
	want := `{"boot_id":"fixture-boot","monotonic_ns":9007199254740993,"observed_at":"2026-11-17T17:00:00Z","trusted":true}`
	if err != nil || string(canonical) != want {
		t.Fatalf("canonical = %q, %v", canonical, err)
	}
	digest, err := managerTimerDomainDigest("Clock", object)
	if err != nil || hex.EncodeToString(digest[:]) != "c531678347e811f0d66274843f12d30f93ef0a59bc7b39a8e7dacec3de88c240" {
		t.Fatalf("domain digest = %x, %v", digest, err)
	}
	object, err = decodeManagerTimerObject([]byte("{\"text\":\"α & < > \\u2028 \\u2029 \\ud83d\\ude00\",\"empty\":[],\"missing\":null}\n"))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err = canonicalManagerTimerValue(object)
	want = "{\"empty\":[],\"missing\":null,\"text\":\"α & < > \u2028 \u2029 😀\"}"
	if err != nil || string(canonical) != want {
		t.Fatalf("literal Unicode canonical = %q, %v", canonical, err)
	}
}

// Forbidden parse neighbors fail before a typed record or any effect exists;
// nearby multiline, nested, escaped Unicode and inclusive-size positives pass.
func TestManagerTimerFrameRefusals(t *testing.T) {
	for _, input := range []string{
		"{\"n\":1.5}\n", "{\"n\":1e0}\n", "{\"n\":9223372036854775808}\n", "{\"n\":-1}\n",
		"{\"x\":1,\"x\":1}\n", "{\"x\":1,\"\\u0078\":1}\n", "{\"a\":{\"x\":1,\"x\":2}}\n",
		"{\"x\":\"\\ud800\"}\n", "{\"x\":\"\\udc00\"}\n", "{\"x\":\"\\ud800\\u0041\"}\n",
		"{\"x\":\"\xff\"}\n", "{}", "{}{}\n", "[]\n", "null\n", "{\"x\":}\n",
	} {
		t.Run(input, func(t *testing.T) {
			object, err := decodeManagerTimerObject([]byte(input))
			if err != managerTimerInvalid || object != nil {
				t.Fatalf("object = %#v, error = %v", object, err)
			}
		})
	}
	for _, input := range []string{"{\"n\":9223372036854775807}\n", "{\"n\":-0}\n", "{\"x\":\"\\\\ud800\"}\n", "{\"a\":[null,{\"b\":true}]}\n"} {
		if _, err := decodeManagerTimerObject([]byte(input)); err != nil {
			t.Fatalf("valid neighbor %q: %v", input, err)
		}
	}
	valid := append([]byte("{}"), bytes.Repeat([]byte(" "), managerTimerFrameLimit-3)...)
	valid = append(valid, '\n')
	if _, err := decodeManagerTimerObject(valid); err != nil {
		t.Fatalf("inclusive frame limit: %v", err)
	}
	invalid := append(append([]byte{}, valid[:len(valid)-1]...), ' ', '\n')
	if object, err := decodeManagerTimerObject(invalid); err != managerTimerFrameTooLarge || object != nil {
		t.Fatalf("oversize = %#v, %v", object, err)
	}
	deep := strings.Repeat(`{"a":`, 40) + `0` + strings.Repeat(`}`, 40) + "\n"
	if _, err := decodeManagerTimerObject([]byte(deep)); err != managerTimerInvalid {
		t.Fatalf("excess nesting: %v", err)
	}
}
