package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

// RunOwner observes only an already acknowledged assignment. The fork renders
// and submits its instruction through HostInputWriter before joining here.
func (h *codexRunner) RunOwner(ctx context.Context, request ProviderRunRequest) (*ProviderRunResult, error) {
	if strings.TrimSpace(request.Prompt) == "" {
		return nil, fmt.Errorf("managed Codex owner prompt is required")
	}
	call, finish, err := h.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if err := historyguard.LockMutex(call, &h.runMu); err != nil {
		return nil, err
	}
	defer h.runMu.Unlock()
	ctx = call
	h.mu.Lock()
	threadID, managedTurnID := h.snapshot.ThreadID, h.snapshot.ManagedTurnID
	client, allowAcknowledgementReconciliation := h.ownerClient, h.ownerStarted
	h.mu.Unlock()
	if managedTurnID == "" {
		return nil, fmt.Errorf("%w: native Codex owner requires an acknowledged assignment", ErrInputAcknowledgementMismatch)
	}
	if client == nil {
		client, err = h.dialInitialized(ctx)
		if err != nil {
			return nil, err
		}
		if err := h.resumeOwnerClient(ctx, client, threadID); err != nil {
			h.releaseClient(client)
			return nil, err
		}
		client.drainNotifications()
		h.markConsumerEvidenceIncomplete()
	}
	defer func() {
		h.mu.Lock()
		if h.ownerClient == client {
			h.ownerClient = nil
			h.ownerStarted = false
		}
		h.mu.Unlock()
		h.releaseClient(client)
	}()
	var (
		idleSince          time.Time
		turnInvisibleSince time.Time
		observedTurnStatus string
		observedEvents     = make(map[string]bool)
		lastSummary        string
		output             strings.Builder
		usage              ProviderUsage
	)
	ticker := time.NewTicker(h.managedOwnerPollPeriod())
	defer ticker.Stop()
	for {
		thread, readErr := readCodexOwnerThread(ctx, client, threadID)
		if readErr != nil {
			return &ProviderRunResult{
				ExitCode:    1,
				Status:      ProviderRunFailed,
				Detail:      readErr.Error(),
				Recoverable: true,
				Output:      output.String(),
				Usage:       usage,
			}, nil
		}
		if thread.ID != threadID {
			return nil, fmt.Errorf("%w: native Codex owner read thread", ErrInputAcknowledgementMismatch)
		}
		if err := validateCodexTurnHistory(thread.Turns); err != nil {
			return nil, err
		}
		listedTurns, listErr := listCodexRunnerTurns(ctx, client, threadID)
		if listErr != nil {
			return &ProviderRunResult{
				ExitCode:       1,
				Status:         ProviderRunFailed,
				ProviderStatus: "turn_correlation_unavailable",
				Detail: fmt.Sprintf(
					"Codex thread/turns/list failed while correlating the managed writer: %v",
					listErr,
				),
				Output: output.String(),
				Usage:  usage,
			}, nil
		}
		var (
			notificationStatus    string
			notificationMethods   []string
			notificationCandidate string
		)
		// One drain, two readers. The turn correlator needs the lifecycle events
		// and consumer-repository-observer-v1 needs the item events, and both are
		// in the same batch; draining twice would give each half a stream.
		drained := client.drainNotifications()
		h.appendConsumerNotifications(drained)
		usage, notificationStatus, notificationMethods, notificationCandidate = client.nativeTurnEvidenceFrom(
			drained,
			threadID,
			managedTurnID,
			usage,
			allowAcknowledgementReconciliation,
		)
		if notificationCandidate != "" {
			managedTurnID = notificationCandidate
			h.mu.Lock()
			h.snapshot.ManagedTurnID = managedTurnID
			h.mu.Unlock()
			output.WriteString("provider_turn_identity=reconciled_from_bound_lifecycle\n")
		}
		turn := findCodexTurn(thread, managedTurnID)
		if turn == nil {
			turn = findCodexTurnInList(listedTurns, managedTurnID)
		}
		for _, method := range notificationMethods {
			if observedEvents[method] {
				continue
			}
			observedEvents[method] = true
			output.WriteString("provider_event=")
			output.WriteString(method)
			output.WriteByte('\n')
		}
		if notificationStatus != "" {
			observedTurnStatus = notificationStatus
		}
		if turn != nil && strings.TrimSpace(turn.Status) != "" {
			observedTurnStatus = turn.Status
		}
		turnStatus := observedTurnStatus
		if turnStatus == "" {
			turnStatus = "not_visible"
		}
		if turn == nil {
			if observedTurnStatus != "" {
				// A lifecycle notification is correlated by the exact
				// (threadId, turnId) even when thread/read has not materialized
				// the turn into the rollout view.
				turnInvisibleSince = time.Time{}
			} else if thread.Status.Type == "active" {
				// Codex may keep an acknowledged in-flight turn out of
				// thread/read until the work reaches a terminal state.
				// An active thread is therefore positive evidence that the
				// existing writer is still running, not that its turn was lost.
				turnInvisibleSince = time.Time{}
			} else if turnInvisibleSince.IsZero() {
				turnInvisibleSince = time.Now()
			} else if time.Since(turnInvisibleSince) >= h.managedOwnerTurnVisibilityLimit() {
				return &ProviderRunResult{
					ExitCode:       1,
					Status:         ProviderRunFailed,
					ProviderStatus: "turn_unobservable",
					Detail: fmt.Sprintf(
						"manager-owned Codex turn did not become observable within %s after the thread stopped being active; "+
							"the turn id was retained and a new turn was not started to avoid a dangling second writer; "+
							"stop and relaunch the managed session before retrying",
						h.managedOwnerTurnVisibilityLimit(),
					),
					Output: output.String(),
					Usage:  usage,
				}, nil
			}
		} else {
			turnInvisibleSince = time.Time{}
		}
		summary := fmt.Sprintf(
			"thread_status=%s turn_status=%s",
			thread.Status.Type,
			turnStatus,
		)
		if summary != lastSummary {
			output.WriteString(summary)
			output.WriteByte('\n')
			lastSummary = summary
		}
		// Codex native goals are disabled on managed hosts, so the end of the
		// managed turn is the provider's only completion signal. It stays
		// advisory: spawnruntime verifies the role handoff, the acceptance oracle
		// and outcome evidence, and a run that ended its turn without handing off
		// gets a same-revision successor under the bounded recovery policy.
		if turnStatus == "completed" {
			h.mu.Lock()
			h.snapshot.LastCompletedTurnID = managedTurnID
			h.snapshot.ManagedTurnID = ""
			h.mu.Unlock()
			if blocker, ok := codexTurnBlockedReply(turn); ok {
				return &ProviderRunResult{
					ExitCode:       1,
					Status:         ProviderRunBlocked,
					ProviderStatus: "blocked_reply",
					Detail:         blocker,
					Output:         output.String(),
					Usage:          usage,
				}, nil
			}
			return &ProviderRunResult{
				Status:         ProviderRunComplete,
				ProviderStatus: "turn_completed",
				Output:         output.String(),
				Usage:          usage,
			}, nil
		}
		if turnStatus == "failed" && codexTurnUsageLimited(turn) {
			h.mu.Lock()
			h.snapshot.ManagedTurnID = ""
			h.mu.Unlock()
			return &ProviderRunResult{
				ExitCode:       1,
				Status:         ProviderRunUsageLimited,
				ProviderStatus: "usage_limit",
				Detail:         "Codex provider usage limit requires autonomous recovery",
				Recoverable:    true,
				Output:         output.String(),
				Usage:          usage,
			}, nil
		}
		if thread.Status.Type == "systemError" || turnStatus == "failed" || turnStatus == "interrupted" {
			if turnStatus == "failed" || turnStatus == "interrupted" {
				h.mu.Lock()
				h.snapshot.ManagedTurnID = ""
				h.mu.Unlock()
			}
			return &ProviderRunResult{
				ExitCode:       1,
				Status:         ProviderRunFailed,
				ProviderStatus: turnStatus,
				Detail:         fmt.Sprintf("Codex owner stopped with thread=%s turn=%s", thread.Status.Type, turnStatus),
				Recoverable:    true,
				Output:         output.String(),
				Usage:          usage,
			}, nil
		}
		if thread.Status.Type == "idle" {
			if idleSince.IsZero() {
				idleSince = time.Now()
			} else if time.Since(idleSince) >= codexOwnerIdleLimit {
				return &ProviderRunResult{
					ExitCode:       1,
					Status:         ProviderRunIdle,
					ProviderStatus: "idle",
					Detail:         fmt.Sprintf("manager-owned Codex thread was idle for %s without a completed managed turn", codexOwnerIdleLimit),
					Recoverable:    true,
					Output:         output.String(),
					Usage:          usage,
				}, nil
			}
		} else {
			idleSince = time.Time{}
		}
		select {
		case <-ctx.Done():
			reconciled := h.reconcileManagedTurn(threadID, managedTurnID)
			if reconciled == "completed" {
				h.mu.Lock()
				h.snapshot.LastCompletedTurnID = managedTurnID
				h.mu.Unlock()
			} else if reconciled == "failed" || reconciled == "interrupted" {
				h.mu.Lock()
				h.snapshot.ManagedTurnID = ""
				h.mu.Unlock()
			}
			return &ProviderRunResult{
				ExitCode:       1,
				Status:         ProviderRunFailed,
				ProviderStatus: "context_cancelled_reconciled_" + reconciled,
				Detail:         ctx.Err().Error(),
				Recoverable:    true,
				Output:         output.String(),
				Usage:          usage,
			}, nil
		case <-ticker.C:
		}
	}
}

func (h *codexRunner) managedOwnerPollPeriod() time.Duration {
	if h.ownerPollPeriod > 0 {
		return h.ownerPollPeriod
	}
	return codexOwnerPollPeriod
}

func (h *codexRunner) managedOwnerTurnVisibilityLimit() time.Duration {
	if h.ownerTurnVisibilityLimit > 0 {
		return h.ownerTurnVisibilityLimit
	}
	return codexOwnerTurnVisibility
}

func (h *codexRunner) resumeOwnerClient(ctx context.Context, client *codexRPCClient, threadID string) error {
	raw, err := client.request(ctx, "thread/resume", map[string]any{"threadId": threadID, "persistExtendedHistory": true})
	if err != nil {
		return err
	}
	var response struct {
		Thread codexThread `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID != threadID {
		return ErrInputAcknowledgementMismatch
	}
	client.subscribedThreadID = threadID
	return nil
}

func (h *codexRunner) reconcileManagedTurn(threadID, turnID string) string {
	ctx, cancel := context.WithTimeout(h.life, 3*time.Second)
	defer cancel()
	client, err := h.dialInitialized(ctx)
	if err != nil {
		return "unavailable"
	}
	defer h.releaseClient(client)
	thread, err := readCodexThread(ctx, client, threadID)
	if err != nil || thread.ID != threadID {
		return "unavailable"
	}
	turn := findCodexTurn(thread, turnID)
	if turn == nil || strings.TrimSpace(turn.Status) == "" {
		return "missing"
	}
	if validateCodexTurnHistory(thread.Turns) != nil {
		return "unavailable"
	}
	return turn.Status
}
