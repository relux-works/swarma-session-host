package main

import (
	"strings"
	"testing"
)

func runSession(t *testing.T, f *fixture, input string) string {
	t.Helper()
	var out strings.Builder
	if err := f.run(strings.NewReader(input), &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	return out.String()
}

func TestSyntheticSession(t *testing.T) {
	f, err := configure("", "")
	if err != nil {
		t.Fatal(err)
	}
	out := runSession(t, f, strings.Join([]string{
		`{"id":1,"method":"initialize","params":{"clientInfo":{"name":"x"}}}`,
		`{"method":"initialized","params":{}}`,
		`{"id":2,"method":"thread/start","params":{"model":"m","modelProvider":"p","cwd":"/tmp/ws"}}`,
		`{"id":3,"method":"turn/start","params":{"threadId":"fixture-thread","model":"m","effort":"e","input":[{"type":"text","text":"hi"}]}}`,
		"",
	}, "\n"))
	for _, want := range []string{`"userAgent"`, `"thread":{"id":"fixture-thread"}`, `"status":"inProgress"`, `turn/started`, `turn/completed`, `"status":"completed"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "approvalPolicy") {
		t.Errorf("synthetic session must not echo a policy:\n%s", out)
	}
}

func TestYoloSessionEchoesPolicy(t *testing.T) {
	f, err := configure("", "")
	if err != nil {
		t.Fatal(err)
	}
	out := runSession(t, f, strings.Join([]string{
		`{"id":1,"method":"initialize","params":{}}`,
		`{"id":2,"method":"thread/start","params":{"model":"m","modelProvider":"p","cwd":"/tmp/ws","approvalPolicy":"never","sandbox":"danger-full-access"}}`,
		"",
	}, "\n"))
	for _, want := range []string{`"approvalPolicy":"never"`, `"cwd":"/tmp/ws"`, `"type":"dangerFullAccess"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRefusals(t *testing.T) {
	if _, err := configure("relative/marker", ""); err == nil {
		t.Error("relative marker accepted")
	}
	if _, err := configure("", "999999"); err == nil {
		t.Error("oversized delay accepted")
	}
	f, _ := configure("", "")
	var out strings.Builder
	if err := f.run(strings.NewReader(`{"id":9,"method":"unknown/method","params":{}}`+"\n"), &out); err == nil {
		t.Error("unknown method accepted")
	}
}
