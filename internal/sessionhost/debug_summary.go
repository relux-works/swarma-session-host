package sessionhost

import (
	"encoding/json"
	"errors"
)

// debugSummary is the only diagnostic export: fixed classes, bounded counters,
// relative durations and verdicts. It carries no identifiers, paths, grant
// material, bodies or payload-derived digests, and there is no raw transcript
// export command at all.
type debugSummary struct {
	Contract       string         `json:"contract"`
	Writer         string         `json:"writer"`
	WriterRevision uint64         `json:"writer_revision"`
	Attached       bool           `json:"attached"`
	Grants         int            `json:"grants"`
	Retired        bool           `json:"retired"`
	Results        debugCounts    `json:"results"`
	Inputs         map[string]int `json:"inputs"`
	Interactions   map[string]int `json:"interactions"`
	Tail           debugTailStat  `json:"tail"`
	Journal        debugJournalSt `json:"journal"`
	Rejections     map[string]int `json:"rejections"`
	// OldestDebtSeconds is the age of the oldest unacknowledged terminal fact
	// relative to the host clock, or -1 when no debt is outstanding.
	OldestDebtSeconds int64 `json:"oldest_debt_seconds"`
}
type debugCounts struct {
	Total, Pending, Acknowledged, Permitted int
}
type debugTailStat struct {
	Frontier, Lost uint64
	Cause          string
	Bytes, Events  int
	Closed         bool
}
type debugJournalSt struct {
	Used, Reserved int64
	Sequence       uint64
	Torn, Failed   bool
}

func (h *Host) debugSummaryJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil || h.state.Debug == nil {
		return nil, errors.New("attach_not_enabled")
	}
	d := h.state.Debug
	s := debugSummary{Contract: debugContract, Writer: d.Writer, WriterRevision: d.WriterRevision, Attached: h.debug.viewer != "", Grants: len(h.debug.grants), Retired: d.Retired,
		Inputs: map[string]int{}, Interactions: map[string]int{}, Rejections: map[string]int{}, OldestDebtSeconds: -1}
	now := h.debug.now()
	for _, r := range d.Results {
		s.Results.Total++
		if r.NotificationPending {
			s.Results.Pending++
			if age := int64(now.Sub(r.ObservedAt).Seconds()); age >= 0 && (s.OldestDebtSeconds < 0 || age > s.OldestDebtSeconds) {
				s.OldestDebtSeconds = age
			}
		}
		if r.Ack {
			s.Results.Acknowledged++
		}
		if r.Permitted {
			s.Results.Permitted++
		}
	}
	for _, in := range d.Inputs {
		s.Inputs[in.Stage]++
	}
	for _, x := range d.Interactions {
		s.Interactions[x.State]++
	}
	for code, n := range h.debug.rejections {
		s.Rejections[code] = n
	}
	t := h.debug.tail
	t.mu.Lock()
	s.Tail = debugTailStat{Frontier: t.next, Lost: t.lost, Cause: t.cause, Bytes: t.used, Events: t.events.Len(), Closed: t.closed}
	t.mu.Unlock()
	j := h.debug.journal
	s.Journal = debugJournalSt{Used: j.used, Reserved: j.reserved, Sequence: j.sequence, Torn: j.torn, Failed: j.failed}
	return json.Marshal(s)
}
