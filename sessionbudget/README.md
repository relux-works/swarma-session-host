# Provider budget accounting

This package supplies `sessiongoal.BudgetBinding` through a lease-bound
`sessionmanager.GoalBindingFactory`. `NewFactory(lifetime, goals, store, source)`
receives the actual goal service, a private durable store, and runtime accounting
hydration. `Close(ctx)` cancels and joins its own observation workers. It does not
own the host's controller epochs, runner lifetime or goal authority.

Muse accounting uses the real `NativeGoalObserver`, `HostInputWriter` and
`MuseObservationSource`. Goal delivery obtains its own existing observation
scope; the budget wrapper never nests another writer acquisition around it.
A tracking writer retains exact inputs, receipts and matching acknowledgements.
Only that evidence promotes a selected positive or explicitly unlimited budget.
A later error retains the acknowledged recovery obligation. Missing or mismatched
ACKs retain the prior allocation and unresolved input, without a reset.

Usage is cumulative `TotalTokens`; an absent cumulative value is unknown. A
counter regression is refused. Revisions of one goal carry its baseline; a
matched clear forgives that goal's retained spend, and a new activation starts
from then-current usage. A threshold check observes the current objective under
the real host fence before issuing typed `goal/pause`. A matched pause alone
latches budget-pause provenance. An unacknowledged pause remains unresolved;
observing a paused status cannot fabricate that acknowledgement. Native resume
re-arms enforcement. This is observed host accounting, not a native hard cap.

Codex retains selected positive/none budget rendering through the actual goal
service's input path with native goals disabled. Claude retains revision and
condition/token validation through its actual `/goal` path. This package does not
claim a numeric spending sink or hard cap for either provider. ACK timing is
independent of spending budget.

`BindCodexGoal` binds an actual provider revision renderer to this same service
and selected text budget. A truthful no-Submit deferral preserves its complete
error tree without inventing an acknowledgement. Once input is offered, the
normal retained-receipt checks remain mandatory. It does not select managed
owner mode, install native goals or create a numeric accounting sink for Codex.

`Store.Restore` can restore **this store's** retained accounting under a current
observer/lease and exact runtime-supplied native identity. New stores admit only
an observed empty native goal. They cannot adopt existing native work as fresh.
An unresolved effect blocks rebinding under a new epoch. The runtime's
`AccountingSource.HydrateBudget(ctx, lease, observer, store)` must resolve the
actual session and persisted accounting identity before Muse binding. Missing
hydration fails closed; no canned source is provided.

`LegacySource` consumes `LegacyMuseAccountingSnapshot` and its real native
accounting field group from `LegacyRecordCodec`; it retains the full original
ledger, pause provenance and other native fields. Hydration verifies actual
runtime native identity, current observer/lease, exact native objective and known
counters. A no-change export retains original bytes. Counter-reset rejection is
retained as uncertainty, without an active allocation or invented zero counters.

The runtime supplies `LegacyImages` through its normal current-lease record
transaction. Image read/CAS and codec merge occur outside the native observer
fence. Missing record access, stale images and persistence errors remain explicit
refusals or recovery obligations. The actual core codec performs reference
preflight before input; the sink never renames an immutable reference or widens
the published wire validator. An incompatible historical experimental reference
cannot become a new valid reference through budget conversion.

Text-child accounting remains in the exact original provider image and is
preserved by the real codec, not replaced by native cumulative accounting.
A typed archival image and this library's fixture qualification do not grant
normal active-record admission, restore a process, or implement the host's record
adapter. The host must supply that real normal decode/commit/recovery path;
there is no arbitrary raw writer or default-success runtime adapter here.

## Validation tools

Use the repository's pinned module closure and Go toolchain. Narrow commands are
`go test -p 1 -parallel 1 -count 1 ./sessionbudget`,
`go build -p 1 ./sessionbudget`, `go vet -p 1 ./sessionbudget`, and
`gofmt -d sessionbudget`. Isolated tests use the real durable goal/accounting
stores and core receipt checker with typed observer/writer fixtures. They do not
launch a provider or qualify a native daemon. Evidence and temporary composition
fixtures belong under `.temp/`, outside package source. The accounting store
requires private0700 directories/private0600 records and uses atomic writes,
fsync and an OS file lock on the supported Unix hosts.

The actual SupportsNativeGoals capability distinguishes native Muse accounting
from PTY/text backends that retain generic observation methods. Without native
capability, an absent goal follows real no-delivery reconciliation and creates
no native account. Active, pending or unsettled native-goal assignments refuse
before hydration/delivery. Nonactive heads require a real applied receipt.
The closed budget binding never invokes delivery. This does not supply PTY text
reflection or a native hard cap. Missing metadata retains actual native-source
validation; it is not evidence of a goal-less PTY.
