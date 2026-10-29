package musepty

import (
	"testing"
)

func TestMusePTYInboxCorrelationRefusesMalformedRecords(t *testing.T) {
	for _, which := range []string{"missing_source", "wrong_drain", "invented_inbox_item_id"} {
		t.Run(which, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("started")))
			queue := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
			p := queue["payload"].(map[string]any)
			if which != "wrong_drain" {
				delete(p, "source_run_record_id")
			}
			if which == "invented_inbox_item_id" {
				p["event"].(map[string]any)["inbox_item_id"] = "forged"
			}
			appendMuseRecord(t, h, queue)
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
			if which == "wrong_drain" {
				// The drain target run is observed so the refusal
				// stays attributable to the wrong item id: the
				// drain-other-item-only mutant repoints exactly it.
				target := museFixtureRecord("inbox_item_drained")["payload"].(map[string]any)["run_id"].(string)
				museEmitRunBounded(t, h, "started", target)
				museEmitRunBounded(t, h, "terminal", target)
				d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
				d["payload"].(map[string]any)["event"].(map[string]any)["item_id"] = museTestUUID("other")
				appendMuseRecord(t, h, d)
			}
			assertMuseNoInput(t, h, home)
		})
	}
}
