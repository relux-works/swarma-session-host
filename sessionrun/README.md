# Captured owner rendering

`Runtime` implements `sessionmanager.CapturedOwnerRunner`. The host owns attachment admission, controller allocation, goal preflight, materialization and durable outcomes. Rendering uses the captured typed writer for assignments and the existing native observation ports for results. No run holds the host epoch fence while waiting for a terminal.

Construct `New(store, settings)` with the actual host settings. `GoalAckTimeout` bounds pending-request resolution; it does not limit spending or owner run duration. Supply `runtime.AccountingSource()` to the budget factory, call `BindBudget(factory)` and `BindHost(host)` before recovery or serving, and configure `HostOptions.GoalBindings = runtime`. The runtime delegates native providers to that same factory and binds actual non-native Muse writers to the provider Text renderer through the real goal service and receipt supplier.

Codex starts an unassigned turn once through the captured writer, checks its native thread/turn receipt, then joins the provider's existing acknowledged-turn observer. `BindOwnerRunGoal` binds the captured owner operation to the same goal service and budget factory. The actual stored revision directive and trimmed owner prompt share one native Start. The service journals the exact submitted bytes before input, retains the returned receipt before consuming Await, and promotes only after independent host receipt checks. A retry uses the exact stored input; changing its owner prompt or borrowing a retained wrong acknowledgement refuses. Joining a combined goal turn requires its local assignment evidence, the real applied revision and the same retained native thread/turn receipt. A goal-only managed turn cannot supply that evidence.

The managed renderer defers without Submit when it has no owner prompt. Ordinary binding still requires the original retained owner-run mode to select that renderer; this mode is absent from the current typed Codex snapshot. Neither an owner label nor a goal reference supplies it. The existing live codec refusal for unsupported retained owner-run state remains in force. Isolated composition tests select the actual renderer explicitly and qualify the service/host continuation, not production hydration of that missing mode or active restart. An unresolved rollback remains a service recovery obligation; it does not manufacture a compensating owner turn. Native completion never accepts or clears the stored goal.

Muse consumes the host's ordered, lease-bound feed. Its restore fold keeps the original accounting group, baseline, ledger and pause facts; native identity comes independently from the actual runner. Accounting reads and CAS use the real host record transaction. `Factory.CheckBudget` joins the existing exact-lease enforcement before terminal attestation. The runtime implements the two-stage `MuseAccountingRestorer`: validation checks the exact original group against the real applied goal journal; hydration consumes a current native Fold/observer into the durable sink and commits through the host record transaction. Core must invoke hydration outside its map and epoch locks before exposing restored inputs. A committed unknown goal observation still needs a genuine observed-empty commit; native data cannot manufacture it. The existing codec's active-state refusal remains authoritative until these callbacks are wired at the actual Restore boundary. A pure fold or archival decoder does not establish active runtime restore support.

Muse PTY and provenance hosts submit one ordinary Text assignment through the captured writer, retaining the `run-observed-after-snapshot` evidence class. The actual read-only `ObserveOwnerRun` then folds only the checked PTY run ID. Committed and physical session/PID/log/provenance identity must match before input; neither the legacy writing `RunOwner` nor native MSP accounting is used for this path. Active PTY goals use the goal service's admitted revision delivery binding, actual writer-native identity and selected-budget validation. Finite and unlimited budgets remain Text instructions for this provider. The optional retained-attempt binding reuses the exact journaled input, command identity and physical correlation in a fresh scope; foreign or uncertain evidence cannot become applied.

## Tools and checks

Use the repository's selected Go SDK and matching `gofmt`:

```sh
gofmt -w sessionrun/*.go sessionbudget/check.go sessionbudget/check_test.go
go test -p 1 -parallel 1 -count 1 -run '^TestPendingCodex' ./sessionrun
go test -p 1 -parallel 1 -count 1 ./sessionrun
go test -p 1 -parallel 1 -count 1 -run '^TestBudgetCapturedCheckRequiresExistingLease$' ./sessionbudget
go build -p 1 ./sessionrun ./sessionbudget
go vet -p 1 ./sessionrun ./sessionbudget
```

Run checks through the environment's resource scheduler. Logs and isolated negative-control copies belong outside the package in task-scoped temporary evidence. The provider fixtures use production host epochs, input and durable codec with isolated in-process native I/O; they do not qualify a live provider or daemon. Original executable-provider and full active Restore witnesses remain separate integration obligations.
