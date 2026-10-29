# Session goals

`sessiongoal` owns durable session assignment and working goals. It implements
`sessionmanager.GoalSource` and constructs bindings using the core receipt checker.
It has no task-board dependency. Assignment completion requires an explicit
holder acceptance or a barrier fact relayed by that holder. Native banners,
turn endings and process lifetime events never complete or delete the journal.

## Admission and namespaces

Open a private directory with `Open(directory, authority)`. Submit a typed
`Command` with `Handle`, or use `Execute` to hold the trusted authority fence
through reconciliation. The command contains no caller or owner assertion.
`Authority.WithVerifiedCaller` must supply already verified exact-session grants,
current holder and generation, and fence those facts for the callback. A raw
principal string, shared Unix UID or command payload is insufficient. This
package supplies no authentication, grant issuance or production authority
adapter. A missing supplier refuses commands.

Assignment writes require the current launcher/holder or an operator with the
assignment grant. Acceptance additionally requires the holder. Working goals
require their separate working grant and working principal. Neither namespace
confers the other's rights. Working goals are persisted for tools; they cannot
bind to the provider's single native assignment-goal plane. Unlimited budget
requires a distinct entitled operator grant.

## Durability and native delivery

Each accepted source envelope has a durable digest, caller, holder generation,
reference and receipt. Exact replay is inert; altered replay and stale CAS refuse.
New references use `GOAL-SG-` followed by the uppercase session/namespace digest,
which the preserved native accounting codec accepts unchanged. Historical
experimental `sg-` journals refuse before native delivery; opening never rewrites
immutable references or migrates their acknowledgement tokens.

An OS file lock serializes independent store instances. Journal replacement uses
file sync, rename and directory sync. Directory mode must be0700 and journal/lock
files0600. The journal retains immutable revision and native application history;
a64MiB capacity bound refuses new writes without discarding history. An uncertain
post-rename commit freezes that service instance until reopen.

Construct `NewBinding` from the actual session's controller lease, provider,
`HostInputWriter` and required `BudgetBinding`. The budget adapter must preserve the selected provider's existing revision
behavior before invoking delivery: Muse needs retained usage accounting and
budget-driven pause, Codex carries positive or unlimited budget text, and Claude
uses validated condition/token delivery. No new native spending cap is implied
for Codex or Claude. Prompt text does not substitute for Muse accounting. No
production budget adapter or success default is supplied here. Rendering uses the existing typed Codex, Claude and Muse ports,
revision token format, safe Claude label, and fenced Muse set/edit observation.
The core verifies submitted receipt provenance, lease, acknowledgement and
correlation before a revision can become applied.

Attempts and acknowledgements are durable. A retry under the same lease retains
its input identity and exact persisted payload, including the Muse set/edit choice,
and relies on the real writer's idempotence. An unresolved
attempt under another controller lease refuses further delivery, rather than
blindly resending. An already applied revision is reasserted with a fresh receipt
after controller replacement. Failed durable promotion after acknowledgement
records an attempted rollback to the previous applied revision; an unresolved
rollback requires host recovery and prevents further commands. A successfully
acknowledged rollback retires its attempt with its durable receipt. The stored
head remains; reapplication receives a fresh input and native command identity,
so restoration after goal-plane clear cannot borrow an old apply acknowledgement.
This package does
not claim automatic resolution of an unknown native effect.

## Host integration

The host must inject verified admission and real budget adapters.
`BindingFactory{Service: service, Budget: budget}` implements the real core
`GoalBindingFactory`; configure it as `HostOptions.GoalBindings` alongside
`HostOptions.Goals = service`. After installing
the exact session writer/controller, it must construct that session's binding and
reconcile `Owner(sessionID, Assignment)` before exposing the attachment. Recovery
must follow the same lease and failure rules. Setting `HostOptions.Goals` alone
does not wire that orchestration. Read/inspection methods are host-internal ports,
not externally authorized endpoints. The host must enforce its configured goal
plane gate before invoking these internal delivery methods. Bridge transport and wire envelopes are
separate integrations; this package does not qualify a live bridge or daemon.

## Protocol8 goal transport

On Unix, `NewProtocol8Handler(service, runtime, proof)` supplies the original
`POST /v1/goals/apply`, `/v1/goals/rollback`, `/v1/owners/reconcile`, and owner
activation begin/end routes. Compose it beneath the core control server's
real authentication, lifetime and drain boundary. This transport does not
install a listener or infer rights from a shared UID, owner body or requester PID.
Each operation also requires the service's verified exact-session assignment
caller fence. It never creates an assignment from a legacy revision payload.

`Protocol8Runtime` is a required physical host integration, not an implementation
provided here. Its effect-free lookup must hold the actual owner reservation,
current controller and request-context binding, enforce the configured goal
plane gate, and preserve the physical record/snapshot bookkeeping before a
successful return. Missing runtime or current binding refuses before delivery.
The session projection must come from the real host; a test reservation fixture
is not an admitted runtime. Production owner/session mapping and activation
reservation integration remain required.

Legacy revision bytes are retained intact. Apply reflects only the latest
already-stored assignment revision. Rollback may restore a stored historical
revision or clear native state while retaining intent. `Protocol8RevisionProof`
must validate the complete original envelope and its mapping to that immutable
revision; no canonical DTO or ledger is embedded here. An absent proof refuses
before native effects. This package supplies no production legacy proof adapter.
The original single-value bounded decoder behavior, response fields and fixed
private-error refusals remain explicit. Native descendant-verified deferred
acknowledgement is not implemented by this transport: a PID does not synthesize
an acknowledgement or a deferred outcome. No whole-protocol compatibility or
live daemon qualification is implied by these library route checks.

## Tools and checks

Use the repository Go SDK and formatter. With the matching core and real module
composition present, run these narrow commands from the module root:

```sh
go test -json -p=1 -parallel=1 -count=1 ./sessiongoal
go test -race -p=1 -parallel=1 -count=1 -run '^TestGoalCASReplayAndRestart$' ./sessiongoal
go build -p=1 ./sessiongoal
go vet -p=1 ./sessiongoal
go list -deps ./sessiongoal
gofmt -w sessiongoal/*.go
```

Tests use isolated journals and explicitly trusted in-process admission/writer
fixtures. They exercise library policy, durable recovery and the actual core
receipt checker; they do not prove external authentication, provider budget
accounting or native runtime behavior. Validation logs, exit codes and isolated
control variants belong in task-local temporary evidence outside product source.
