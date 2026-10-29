package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Explicit adoption keeps the original public code and actionable refusal;
// exact/case-normalized thread identity and instruction-source receipts survive.
func TestCodexHostResumeFailureIsPublicAndActionable(t *testing.T) {
	for _, mode := range []string{"refused", "malformed", "wrong-thread", "accepted", "case-normalized"} {
		t.Run(mode, func(t *testing.T) {
			server := newCodexContextRPCServer(t, func(q codexRPCRequest) (any, []map[string]any, error) {
				if q.Method != "thread/resume" {
					t.Fatalf("unexpected %s", q.Method)
				}
				params := q.Params.(map[string]any)
				if params["threadId"] != "THREAD-1" || params["cwd"] != "/workspace" || params["model"] != "model" || params["approvalPolicy"] != "never" || params["sandbox"] != "read-only" {
					t.Errorf("lost thread launch policy %#v", params)
				}
				switch mode {
				case "refused":
					return nil, nil, fmt.Errorf("fixture refusal")
				case "malformed":
					return "invalid", nil, nil
				case "wrong-thread":
					return map[string]any{"thread": map[string]any{"id": "other"}}, nil, nil
				}
				id := "THREAD-1"
				if mode == "case-normalized" {
					id = "thread-1"
				}
				return map[string]any{"thread": map[string]any{"id": id}, "instructionSources": []string{"fixture-source"}}, nil, nil
			})
			defer server.Close()
			client, err := dialCodexRPC(context.Background(), websocketTestURL(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			defer client.close()
			thread, sources, err := openCodexRunnerThread(context.Background(), client, CodexRunnerSnapshot{ThreadID: "THREAD-1"}, LaunchSpec{Cwd: "/workspace", ToolPolicy: ClientToolPolicy{Approval: "never", Sandbox: "read-only"}}, "model")
			if mode == "accepted" || mode == "case-normalized" {
				if err != nil || thread != "THREAD-1" || len(sources) != 1 || sources[0] != "fixture-source" {
					t.Fatalf("lost receipt %s %v %v", thread, sources, err)
				}
				return
			}
			var public *PublicError
			if !errors.As(err, &public) || public.Code != CodexErrorThreadResumeFailed || !strings.Contains(err.Error(), "THREAD-1") || thread != "" || sources != nil {
				t.Fatalf("resume refusal %s %v %v", thread, sources, err)
			}
		})
	}
}

// A builder's launch policy remains empty even when the admitted plan carries
// ordinary values; ordinary neighboring launches retain both policy fields.
func TestCodexBuilderThreadParamsCannotOverridePrivatePermissionProfile(t *testing.T) {
	plan := &PrimarySessionLaunchPlan{}
	plan.Resolved.Approval.Value = "never"
	plan.Resolved.Sandbox.Value = "danger-full-access"
	if got := codexAppliedToolPolicy(plan, false); got != (ClientToolPolicy{}) {
		t.Fatalf("builder policy widened %#v", got)
	}
	if got := codexAppliedToolPolicy(plan, true); got.Approval != "never" || got.Sandbox != "danger-full-access" {
		t.Fatalf("ordinary policy dropped %#v", got)
	}
}
