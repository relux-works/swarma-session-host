package musepty

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func baselineGolden(t *testing.T, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	path := filepath.Join("testdata", "baseline", name+".golden.json")
	if os.Getenv("TASK_BOARD_UPDATE_BASELINE") == "1" {
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("baseline %s differs:\nwant %s\ngot %s", path, want, data)
	}
}

// Pins goal text actually submitted to the existing Muse PTY peer for apply/update/rollback/clear; invalid goals submit nothing.
func TestBaselineMusePTYGoalInput(t *testing.T) {
	h, home := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var rows []map[string]any
	for _, step := range recordedGoalInputs(t) {
		err := h.DeliverGoalText(ctx, step.Submitted)
		if err != nil {
			t.Fatal(err)
		}
		input, err := os.ReadFile(filepath.Join(home, "peer-submit"))
		if err != nil {
			t.Fatal(err)
		}
		// The peer prefixes a transport sequence, which is not part of the submitted text.
		_, text, ok := strings.Cut(string(input), ":")
		if !ok {
			t.Fatal("missing peer sequence")
		}
		if strings.Contains(text, "/goal") {
			t.Fatal("native Muse goal command submitted")
		}
		rows = append(rows, map[string]any{"operation": step.Operation, "submitted": text})
		museSettleScriptedIdle(t, h, home)
	}
	before, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.DeliverGoalText(ctx, ""); err == nil {
		t.Fatal("invalid goal admitted")
	}
	after, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("invalid goal submitted input")
	}
	baselineGolden(t, "goal-input", rows)
}

// Pins Muse's distinct launch-envelope decoder and provenance refusals without starting a provider.
func TestBaselineMuseLaunchDecode(t *testing.T) {
	home := "/baseline/home"
	launch := museFactoryPeerLaunch(t, home)
	raw, err := json.Marshal(launch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMuseSessionLaunch(raw)
	if err != nil {
		t.Fatal(err)
	}
	var refusals []string
	for _, bad := range [][]byte{
		append(append([]byte{}, raw...), []byte(" {}")...),
		[]byte(strings.TrimSuffix(string(raw), "}") + `,"foreign":true}`),
		[]byte(strings.Replace(string(raw), launch.FragmentSHA256, strings.Repeat("0", 64), 1)),
	} {
		_, err := DecodeMuseSessionLaunch(bad)
		if err == nil {
			t.Fatal("invalid Muse launch admitted")
		}
		refusals = append(refusals, err.Error())
	}
	baselineGolden(t, "launch-decode", map[string]any{"decoded": decoded, "refusals": refusals})
}
