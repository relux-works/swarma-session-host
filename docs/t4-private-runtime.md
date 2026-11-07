# T4 private runtime candidate and cutover decisions

This implementation candidate continues Host `e58d5df5` and leaves Source
`0a19402a` unchanged. It is a local candidate, not an independent review or a
production activation. The separately reviewed SH1 component retains its original
signed inputs. That acceptance is not acceptance of this composition.

## Runnable Linux build

From the Host repository, with the task's Go bounds:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go build -p 2 -tags t4fixture -o /tmp/t4-private-fixture ./cmd/t4-private-fixture
GOMAXPROCS=2 /tmp/t4-private-fixture -mode accepted
GOMAXPROCS=2 GOFLAGS=-p=2 go test -race -p 2 -tags t4fixture ./cmd/t4-private-fixture -count=1 -v -timeout 90s
```

This executable calls the actual non-test private composition. It uses a pinned
self-executable protocol child, the real operator-file T1 gate, runner.Start,
Codex parser, T2 retained-object handoff, manager admission, Core correlator,
notification stream and durable Host journal. It observes the binding, triggers
one accepted intent and joins cleanup before returning JSON. It takes no native
executable, caller UID, Source connection, payload or credentials from argv.
The optional `-root` must be a fresh private fixture directory; supplied roots
are preserved for inspection. The default temporary root is removed on success.

All Source enrollment/provider selection and Core documents in this binary are
explicit fakes. The explicit `single-trust-unit` profile uses the actual current
UID for S=C and reports **shared UID, no per-agent isolation**. It invents no
caller UID. The real T1 file checks provide no custody boundary against same-UID
writers. See [the profile contract](single-trust-unit.md).
The build tag excludes both fake implementations and executable from ordinary
product builds. This is a runnable **synthetic Linux composition**, not the
missing real Source/native Linux cutover.

Process cases: accepted; policy-refused (no child); source-refused (no endpoint
transfer); ack-refused after Host installation; wrong full binding; native parser
never ready; Source revoked; continuing parent canceled; existing-state restart
refused without journal mutation or another Source preparation. Each child-bearing
case checks process absence after joined cleanup and socket removal. The joined
T1 and T2 completions are checked on successful startup. The canceled-close unit
case holds provider cleanup open and proves the canceled waiter does not finish
the runtime early or abandon cleanup.

The existing seven-case cross-repository Source harness now invokes this same
private composition instead of assembling Start/owner/handoff/manager itself.
Its real Source T3 consumer/enrollment/ledger path is unchanged; deterministic
provider origin and the installed control handshake remain fixture assumptions.
This preserves Source custody and does not create another T3 implementation.

## Exact in-process runtime contract

`openPrivateComposition(parent, request, expires, config, dependencies, kind,
body)` is package-private. Its result owns one newly created execution. There is
no network listener, exported authority constructor or runtime selection route.

The dependencies are explicit `NotificationBroker` and `managerCoreRead`. The
explicit profile/config selects and constructs its own policy; an injected
policy cannot substitute for that selection. Missing dependencies, an expired/canceled owner or request,
and every method except launch refuse before state creation or process effects.
The caller at this seam is trusted composition code, not the external caller.
No request field constructs policy, Source origin or Core authority. Basic
configuration admission constructs the policy; all subsequent returns close its
file handles. After successful transfer those handles have already been closed
by T1 because they gate the completed Start, not renewable runtime authority.

The selected config supplies the record directory, pinned launcher/environment,
fresh state directory, runner socket, opaque native selection, admitted native
release/permission policy and bounded I/O/poll/readiness intervals. Selection of
these inputs remains the integrating owner's responsibility. Socket/workspace/
execution correlation is checked through existing T1/T2 contracts. The actual
retained claim supplies the execution and physical workspace to the Host parser.
The request cannot substitute either through a second configuration field.

The implemented order is:

1. Require all trusted dependencies, explicit `single-trust-unit`/`plain-pathname-v1`
   selection and its frozen configuration binding, then reserve a fresh state directory.
2. Open the real T1 handler and claim the one complete launch operation; its
   operator-file policy gates runner.Start through the existing two checks.
3. Retain that handler's actual custody and open the continuing Host against its
   actual execution/socket/workspace. Native release, permissions and selection
   come from the explicit trusted configuration.
4. Wait, within a finite bound, for actual parser FreshThread and consumed output
   with no pending native action. Waiting never consumes the T2 handoff slot.
5. Open the continuing record source and acknowledge T2 once while serialized
   with T1 cleanup. Only successful acknowledgment transfers cleanup duty.
6. Install the injected Core reader before manager Prepare; run the existing
   full record/Core/profile/workflow and Source-provider preparation checks.
7. Bootstrap one endpoint; require the actual live enrollment callback and the
   broker's successful completion/installed ordering before returning ready.

T2 still rechecks every physical conjunct at transfer and manager use. Fresh
parser observation alone is not admission. Any refusal after acquiring resources
cancels and joins those exact resources before startup returns. Pre-transfer
failures keep T1 duty. Post-transfer failures join T2, manager attachments, Source
completion and original T1 watcher. Cleanup errors are joined with startup errors;
refusal does not mean an earlier possible Start never happened.

`request` controls startup. Successful startup outlives its cancellation. The
finite continuing `parent/expires`, original T1 lifetime, Source lifetime and
attachment loss remain effective. Source/Host loss retires the whole private
composition; it does not allocate another pair or child. `close(wait)` starts
cleanup exactly once, cancels before waiting, and lets an individual waiter time
out while cleanup continues. A final successful close joins the manager, T2 and
T1 paths. Finite Source cancellation/join is an existing dependency obligation,
not a timeout that permits abandoning its ownership.

## Cleanup-r2: private outcome and close observations (M2/M3)

This local successor to `0dfce98e6ce1121dcac3fdaccae1a45d31225aec`
changes the private runtime contract before any installer consumes it. It adds
no public route, wire result code, Source ACK, caller authentication or atomic
exec claim. B2/B3 trust design and Linux birth identity remain separate work;
all composition and provider/native readiness evidence here remains synthetic.

`ordinaryCreateHandler.create` returns the package-private `createOutcome`.
Only retained `createRunnerOwned` and `createContinuingOwned` observations can
return nil error; neither is native success or a new grant. Exact repeats of
`createRefused`, `createStartUnknown` or another terminal state return a nonnil
`*createObservationError` carrying that outcome and unwrapping the existing
`errOrdinaryWireUnavailable`. The private composition explicitly requires
`createRunnerOwned` before borrowing custody. No installer may infer a new
attempt or reuse from either field. Wire dispatch failures keep their existing
error classification. This is an in-memory API change only.

Cleanup starts with a private `createCleanupPending` observation and nonnil
error. `createClosed` is published only after actual T1 or acknowledged T2
cleanup joins; `createCleanupUnknown` retains cleanup failure. Closing admission
alone is never a cleaned-up claim. Request cancellation after the single Start
linearization still retains the possible child and its cleanup obligation.

`NotificationManager.Close(ctx)` now cancels admission and starts exactly one
retained cleanup, outside the caller's wait. Concurrent or later calls join
that same completion. A canceled/deadline waiter returns its own context error
while cleanup continues; only the completed join returns the retained cleanup
result. Failed/unpublished provider cleanup and automatically retired attachment
cleanup errors are retained, so a later successful wait cannot erase UNKNOWN.
The manager retains at most its one operation's attachment, including its closed
result, until the manager is released.

Provider preparation/bootstrap/completion that ignores cancellation may still
prevent total cleanup from completing. It cannot indefinitely block a context-
bounded manager/private-composition close observer, prevent independent retained
Host/runner/wire cleanup from starting, or make the manager completion close
prematurely. A late prepared broker or consumed completion is still owned and
joined; no second preparation, child, pair or handoff is allocated. Independent
attachment lifecycle, prepared-broker and completion cleanup starts together.
Completion close also waits for its nonnil `Done` channel; malformed nil `Done`
remains explicitly unresolved. T2 starts Host, actual runner and wire cleanup
independently and publishes completion only after all three return.

`ManagerAttachment.Close()` retains its existing synchronous, joining API and
has no context parameter. Manager close shields its waiters from that blocking
work. Failed startup still synchronously joins everything acquired before
returning; this change does not promise bounded startup against a noncooperative
provider or cleanup after abrupt process death. There is no watchdog that drops
custody. Observation bounds do not make provider cleanup itself finite.

M1 stays intentional: the first failed retain consumes the handoff slot.
Restoring the original native evidence still refuses a second transfer and
leaves T1 cleanup duty intact. There is no retry, reuse, recovery or adoption.

Focused race cases hold provider Prepare, Bootstrap, completion Close, and Host
close behind explicit test barriers; canceled and deadline concurrent callers
must return before those barriers release. Releasing them proves the actual
late cleanup joins. Refused and possible-Start repeats preserve zero/one Start;
cleanup errors persist through repeated close; T1/T2 completions are observed.
These are inert test processes/providers, not real provider qualification.

## Host restart contract: implemented refusal and proposed decision

The implementation atomically creates `config.stateDir` before Start. An existing
directory, even empty, refuses before opening T1, contacting Source, modifying the
journal or starting a child. Startup failures retain this footprint. The process
case retries after accepted work and joined cleanup: journal bytes remain equal,
Source Prepare stays at one, and the old owned child has exited. There is no
adoption via PID, socket path, serialized receipt, copied binding or restored
Codex checkpoint. Public resource-only manager Open remains unable to supply
ordinary admission.

This directory reservation is not a host-wide durable once ledger or a power-loss
barrier. A different directory is not automatically correlated with the previous
operation. Abrupt process/VM failure is not joined cleanup; a runner child or its
descendants might outlive that failure. No process-supervisor orphan custody or
crash recovery is implemented or claimed.

The smallest proposed first-cutover decision is **one fresh execution per
admitted runtime, with restart/adoption closed**. A supervisor must not silently
restart that operation with another state directory. On runtime loss, retain
existing Source UNKNOWN/reservation and Host intent; report unresolved, keep the
old binding retired, and require the owner-selected recovery decision. This
proposal adds no receipt or automatic retry. It needs explicit owner selection
if this restricted restart behavior is acceptable for the first cutover.

If continuing through Host restart is required, the owner must instead define:
who retains the actual runner/controller and child across runtime death; which
independently authenticated actor may reclaim it; the durable one-operation
claim across directories/processes; how a fresh controller/native session is
admitted under T2's refusal of restored evidence; how the prior binding, endpoint,
Source consumer/generation and unresolved intents are retired or reconciled; and
which acknowledgment transfers cleanup exactly once. Existing state equality
cannot answer those questions. This candidate implements none of that adoption.

## Smallest missing Source contract

No new Host wire format is needed. Host already has `NotificationBroker`,
`PreparedNotificationBroker`, `NotificationBootstrap`, `NotificationEnrollment`
and `NotificationCompletion`; Source T3 already has `ConnectionOrigin`,
`ConnectionTransfer` and `WithConnection`.

The Source owner needs to select and supply three concrete inputs:

| Required owner selection | Minimum exact obligation |
| --- | --- |
| Actual provider-origin constructor and composition site | Produce `ConnectionOrigin` from independently retained Source provider/connection/server, paths, member/configuration/profile, original session/generation/consumer, attachment and finite lease. `Recheck` must observe that actual predicate; copied Host origin fields are insufficient. State which live owning component may invoke it and guarantee one transfer. |
| Installed acknowledgment and failure ordering | Supply T3's required `Installed(context.Context) error`, using the owner's selected actual acknowledgment after `Enrolled` synchronously installs Host service. Define success, refusal/loss, cancellation and which actor cancels/joins when either side has installed. No methods before both callbacks succeed. No default ACK bytes or transport is proposed here. |
| Bridge location and dependency | Select the Host module/version and the Source package that implements the existing broker interfaces. It must retain T3 `WithConnection`'s callback until all finite work joins, map all ten frozen fields unchanged, add only real Source generation/receipt, and return a nonnil owned completion after every endpoint consumption, including errors. |

Source's existing contract owns every nonnil endpoint on every WithConnection
return. Host's existing contract closes an endpoint only when Bootstrap returns
nil completion. The bridge must reconcile that distinction: after calling
WithConnection, even a failed bootstrap has a nonnil completion that owns and
joins it. Prepared.Close must cancel/join a bootstrap in progress. Bootstrap
success must await both live enrollment and actual Installed success without
waiting for a method that cannot run before that success. These are obligations
of the missing bridge, not an implementation selected by this producer.

Source T3 remains at `5f172b2` plus the original T4 qualification fixture
`0a19402a`; no Source production factory, ACK, dependency or caller authority is
added here. Its own owner and independent reviewer must review the eventual
selected bridge.

## Remaining real Linux cutover work

- Select and independently qualify the Source factory/ACK/bridge above.
- First dogfood explicitly selects `single-trust-unit` with S=C=current agent UID,
  no per-agent isolation and no production admission. Production O/S/C principals
  and the UID-switch helper remain separate production prerequisites. Root/group
  custody and caller authentication are not supplied by this profile.
- Select actual Core/native release, permissions, provider and launcher install;
  validate their retained physical relationship under that trust boundary.
- Decide whether fresh-only/no-adoption restart is acceptable or specify the
  missing durable recovery/cleanup contract before implementing it.
- Complete platform evidence in the instructed order: Mac, then the primary Linux
  qualification host, then the secondary Linux host. T6 belongs to the Linux qualification owner;
  this T4 synthetic executable does not duplicate or claim that qualification.
  Current Linux identify peer proof and native birth capture remain unsupported.
- Obtain exact-head independent integration and Source-owner reviews and
  signed landing. The historical bot-authorship hold is resolved. No main landing, push, deployment, credentials,
  real native/provider payloads or paid capacity are authorized here.
