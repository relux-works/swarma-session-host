# Session-plugin wire baseline

Source: task-board `0126cbfc185ce8e9453bb1e4168ecd0199686104`.
Contract: curator-agent-launcher v0.4.0 `268e168114b61aad1776f5b417db3b4a1da13295`,
`FRONT-DOOR.md` §§4–5. These are compatibility pins of actual behavior, not
claims that every normative requirement is implemented.

| Golden | Contract and observations |
| --- | --- |
| `operations.json` | §5.1 JSON framing; §5.2 describe/capabilities, launch, resume, attach, status, list, stop and cancel; §5.3 delegated requests; §5.4 records; §5.7 same-body replay, different-body conflict, keyed exclusion and cancellation fence. Stop refuses both busy and unknown, and admits the idle control without a second start on replay. |
| `refusals.json` | §§5.3/5.6 request validation, unsupported runtime/doorbell, secret/tail refusal, literal code/details/exit, rejection of unknown or launcher-local codes and free detail values. |

The files retain request and response JSON as strings to preserve field order,
newlines and envelope shape. Only the generated handle is replaced by
`SES-<opaque>`: its original spelling must match `SES-[0-9a-f]{32}`, and every
occurrence must be an exact `session` or selector `handle` value. The exact
synthetic `/baseline/workspace` cwd is replaced by `<fixture-cwd>` only in cwd
fields; unexpected occurrences fail. The clock and fake native target are
fixed. No real host path or wall-clock value enters the goldens.

Actual behavior and limits:

- Describe advertises delegated Codex only; Muse/Claude are not qualified by
  this fixture. Exact launch is not advertised.
- An unknown operation fails schema validation with `launch_plan_invalid`
  before the engine's unsupported-operation fallback can run.
- JSON `launch_plan_invalid` exits 6, whereas FRONT-DOOR §5.3 and the inherited
  plan classifier describe exit 2. Do not silently repair this during extraction.
- Engine attach returns private `attachment` JSON, without a public envelope;
  it is not sent through the ordinary reply validator. The command's fd4
  transport is covered separately under `cmd/testdata/baseline-wire`.
- Fake provider state establishes busy/unknown semantics, not real provider
  observation. Cancellation and rejected starts assert no extra provider
  start; busy stop additionally asserts byte-identical ledger and host state.
- Pagination with a non-null random cursor, recovery/crash races, retention
  boundaries, live profile binding, real attach and provider qualification are
  not pinned by these new fixtures. Existing tests remain unchanged.

Regenerate intentionally, from `tools/board-cli`:

```sh
TASK_BOARD_UPDATE_BASELINE=1 go test ./internal/sessionplugin -run '^TestBaselineWire' -count=1
```

Ordinary verification omits `TASK_BOARD_UPDATE_BASELINE`; comparisons fail on
missing fixtures or drift. The updater is never enabled for mutation controls.

Inventory: `TestBaselineWireOperations`, `TestBaselineWireRefusals` in
`baseline_wire_test.go`. The command RPC fixture intentionally consumes the
literal operations file, so regenerate it after an approved engine wire change.
