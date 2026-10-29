package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

// Preserve the original daemon pump cadence and injection timeout. Construction
// alone never starts a provider write loop; the healthy control server owns it.
const sessionNoticeDeliveryInterval = time.Second
const sessionNoticeDeliveryTimeout = 2 * time.Minute

func (h *SessionHost) nudgeSessionNoticeDelivery() {
	select {
	case h.noticeWake <- struct{}{}:
	default:
	}
}

// StartSessionNoticeDelivery starts the original coalescing pump. Its stop
// function cancels and joins this owned loop, including an in-flight injection.
func (h *SessionHost) StartSessionNoticeDelivery(ctx context.Context) func() {
	if ctx == nil {
		ctx = context.Background()
	}
	life, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(sessionNoticeDeliveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-life.Done():
				return
			case <-h.noticeWake:
			case <-ticker.C:
			}
			_ = h.DeliverPendingSessionNotices(life)
		}
	}()
	return func() { cancel(); <-done }
}

// DeliverPendingSessionNotices preserves the existing coalesced render and
// delete-on-success rule. Partial reads are errors, not absence; readable rows
// can still deliver while corrupt queue entries remain in the supplied store.
func (h *SessionHost) DeliverPendingSessionNotices(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := historyguard.LockMutex(ctx, &h.noticeMu); err != nil {
		return err
	}
	defer h.noticeMu.Unlock()
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	ids := make([]string, 0, len(h.sessions))
	for id := range h.sessions {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	sort.Strings(ids)
	var result error
	for _, id := range ids {
		result = errors.Join(result, h.deliverSessionNotices(ctx, id))
	}
	return result
}

func (h *SessionHost) deliverSessionNotices(ctx context.Context, id string) error {
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	record := h.sessions[id]
	attached := false
	for _, session := range h.attachments {
		if session == id {
			attached = true
			break
		}
	}
	notifier, ok := h.runners[id].(ProviderSessionNotifier)
	if record.State != SessionRunning || record.GracefulRetirement || !attached || !ok || h.records.uncertain[id] {
		h.mu.Unlock()
		return nil
	}
	// Hold the existing controller's physical-I/O fence from eligibility through
	// native delivery/queue acknowledgement. Stop/replacement cannot borrow it.
	lease := h.attachmentControllers[id]
	release, err := h.epochs.beginIO(ctx, lease)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	defer release()
	rows, readErr := h.options.Notices.Pending(ctx, id)
	var result error
	result = errors.Join(result, readErr)
	if readErr != nil {
		h.noticeEvent("session_notice_inbox_unreadable", readErr, id, 0)
	}
	payloads := make([]SessionNotice, 0, len(rows))
	receipts := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		if row.ID == "" || seen[row.ID] || row.Payload == nil || row.Payload.SessionID != id || row.Payload.Contract != SessionNoticeContract {
			result = errors.Join(result, fmt.Errorf("%w: session notice payload or receipt", ErrInvalidInput))
			continue
		}
		seen[row.ID] = true
		payloads = append(payloads, *row.Payload)
		receipts = append(receipts, row.ID)
	}
	if len(payloads) == 0 {
		return result
	}
	text := FormatSessionNoticeBatch(payloads)
	if err := ValidateSessionNoticeText(text); err != nil {
		return errors.Join(result, err)
	}
	deliverCtx, cancel := context.WithTimeout(ctx, sessionNoticeDeliveryTimeout)
	err = notifier.DeliverSessionNotice(deliverCtx, text)
	cancel()
	if err != nil {
		h.noticeEvent("session_notice_delivery_failed", err, id, len(payloads))
		return errors.Join(result, err)
	}
	for _, receipt := range receipts {
		result = errors.Join(result, h.options.Notices.Acknowledge(ctx, id, receipt))
	}
	h.noticeEvent("session_notice_delivered", nil, id, len(payloads))
	return result
}

func (h *SessionHost) noticeEvent(event string, cause error, id string, count int) {
	if h.options.Layout.OperatorLogFile == "" {
		return
	}
	logger, err := newOperatorLogger(h.options.Layout)
	if err != nil {
		return
	}
	fields := map[string]any{"session_id": id}
	if count > 0 {
		fields["notices"] = count
	}
	if cause != nil {
		logger.Error(event, cause, fields)
	} else {
		logger.Info(event, fields)
	}
}
