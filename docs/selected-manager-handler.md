# Selected Host manager handler

The executable plugin prefix is `swarma-session-host session-manager`. It uses
the existing launcher registry and `curator-session-manager/1` requests. The
plugin resolves `$XDG_STATE_HOME/swarma-session-host/manager.sock`, or the same
path below `$HOME/.local/state` when the XDG variable is absent. It does not
start a daemon, start a native process, or choose a replacement backend on
transport loss. Installation and live qualification remain separate work.

The handler and retained admission object live in `internal/sessionhost`.
`registerHostPlugin` connects the daemon's privately composed ordinary execution
owner to its one plugin incarnation. No public admission setter, caller verifier,
second manager registry or launcher wire field is introduced. The public
`manager.Open` constructor retains resources only and cannot admit an operation.
The `serve` command and the launcher's fake manager remain fixtures.

The ordinary owner independently admits the complete plan, resolved profile,
policy, selector-free tail and resume selector. After that admission the handler
durably reserves the operation before invoking ordinary execution. Notification
preparation never grants launch authority. Only the ordinary owner returns the
actual retained execution/controller, ordinary session and generation. The
private notification admission retains that object, plugin incarnation, exact
original bytes, kind, operation ID, canonical request digest, Host epoch and
runner instance. Every broker and Host recheck requires current ordinary
admission as well as the candidate's independent record, workspace and Core
checks. This slice admits only `codex_cli`.

Same-execution resume independently checks its current record, workflow and
profile, then reuses its actual attachment. It never calls `Prepare` with the
resume operation against the launch owner's spent claim. A distinct ordinarily
admitted execution may own its first notification claim through `resume` while
retaining the selected native conversation. A replacement controller alone is
not permission to transplant an attachment or allocate a second one.

The configured broker uses the existing `Prepare`, `References`, `Recheck` and
`Bootstrap` interfaces. Its live enrollment callback must independently verify
all twelve binding fields, the actual attachment and finite lease. Host service
installation returns from the callback before the provider's installed ACK.
Opaque provider connection references may differ from public selectors. Endpoint
transfer remains single-use. Nonnil completions own endpoint cleanup even on
failure; nil completions leave cleanup with Host.

The ordinary started record is persisted before notification effects. A failure
before attachment allocation preserves that record with no notification evidence.
A consumer that requested notification treats absence as unresolved. It must not
infer successful fulfillment, invent a reference, restart execution or switch
backends. After allocation the result carries the actual preparing, ready or
terminal projection. Delayed native identity permits three daemon-owned bootstrap
checks within a five-second bound, exclusively before endpoint transfer.

The daemon's private operation ledger retains operations and tombstones for at
least seven days, with a 1024-operation capacity and no automatic eviction. Its
append-only index is synced before the first operation snapshot, so a missing
snapshot cannot become a new operation after restart. Pending writes or unreadable
storage retain uncertainty. Wire-equivalent retries compare the complete canonical
request digest and recorded kind. The private preparation seam still requires
the exact original bytes. Unknown start outcomes never resubmit effects. Replayed
ready metadata after restart is fenced as unresolved until actual live evidence
exists; it cannot reconstruct custody.

`status` accepts exactly one session handle or operation ID. Session status
reads the ordinary owner's current index and record, without admission or
execution. A missing session returns its authoritative `session_host_unavailable`
refusal with `reason=not_found`; an indexed unreadable record returns the fixed
`readable: false`, `state: unknown` session projection. Failed observation never
becomes proof that a session is down. Ordinary records cannot supply notification
authority, and absent attachment evidence remains unresolved. The executable
transport validates a session reply for a session selector and an outcome reply
for an operation selector, retaining refusal classification in either case.

Operation `status` and `cancel` expose durable reconciliation.
Cancellation of an absent or still-prepared operation writes a tombstone before
replying. After reservation, cancellation preserves the actual started/unknown
outcome. It does not stop an execution or clear its obligation. Observation
caller cancellation never ends daemon custody. Launch/resume RPC budgets are
60 seconds; describe/status/cancel budgets are 10 seconds. Daemon custody has an
explicit finite deadline. Cleanup cancels and joins owned work and closes Host
resources while preserving the runner and notification journal. A provider that
ignores cancellation violates the finite-cleanup contract; timed-out waiters do
not make its callback unowned.

Daemon shutdown freezes admission and starts cleanup independently for every
retained manager before waiting for request or provider callbacks. Execution
objects returned during shutdown are retained and cleaned up as well. Completion
is published only after all callbacks and manager cleanups join, followed by
operation-ledger closure. Manager cleanup failures, including unpublished provider
acquisitions and retired attachments, are retained together with ledger closure
failures. Concurrent and repeated close observers receive the same retained result;
a caller's timeout only bounds its wait.

The six `TestSelectedAcceptance` groups exercise the real handler and private
transport using owned inert ordinary and broker adapters. They cover admission,
launch/resume reuse, drift, replay, all binding fields, callback-before-ACK order,
lost replies, restart, unknown effects, cancellation and finite cleanup. They
establish no native start, real member enrollment, process proof or home lease.
The real provider adapter source pin and exact server/root-origin predicate are
still required for live qualification; their absence does not prevent these
Host-side source checks. No productive ordinary owner or provider default is
installed by these tests.
