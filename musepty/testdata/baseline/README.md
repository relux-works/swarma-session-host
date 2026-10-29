# Muse PTY baseline

Source: `0126cbfc185ce8e9453bb1e4168ecd0199686104`.

`TestBaselineMusePTYGoalInput` uses the existing FIFO/frame-synchronized Muse
PTY peer and injected quiet clock. `goal-input.golden.json` records actual
submitted apply/update/clear/rollback/nil rollback text; invalid revisions
must leave the peer's last submission unchanged. No new sleeps or elapsed-time
assertions are used. Only the peer's leading transport sequence (`N:`) is
removed; every submitted character is retained.

`TestBaselineMuseLaunchDecode` records the production Muse-specific decoded
launch envelope and exact unknown-field, trailing-JSON and provenance-digest
refusals in `launch-decode.golden.json`. Paths are fixed `/baseline/home`
fixture values, not live paths; fragment hashes and all other fields are
literal. No normalization is applied.

Muse PTY is registered in production; the unregistered host is the legacy MSP
implementation. Both production compositions are pinned by
`cmd/tb-sessiond/TestBaselineProductionFactories`.

From `tools/board-cli`, regenerate explicitly:

```sh
TASK_BOARD_UPDATE_BASELINE=1 "$CURATOR_GO" test -count=1 -run '^TestBaseline' ./internal/sessionmanager/musepty
```

Without the environment variable the tests only compare existing files. The
real provider, launch probes and arbitrary terminal repaints are not pinned;
the deterministic peer proves submitted text, not model compliance. All
pre-existing Muse PTY corpora remain unchanged. See the parent baseline README
for the inventory, narrow verification and mutation evidence.
