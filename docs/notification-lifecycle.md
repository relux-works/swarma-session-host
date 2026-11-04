# Private notification attachment lifecycle

The Host owns one notification lifecycle per execution. Its private manager
facade validates its owning manager before allocating a close-on-exec Unix
STREAM pair. A second preparation cannot allocate or replace the first pair,
even after loss. The broker endpoint transfers once, only after the actual Host
execution, controller epoch, runner instance and observed native thread are
available. Public preparing status includes only attachment and fixed workflow
references; it contains no partial binding.

The completion path requires a live broker enrollment check, the same admitted
manager, the original attachment, all twelve binding fields and an unexpired
lease. Binding syntax alone is insufficient. Installation and terminal state
changes share the Host mutex. The service may run after successful installation;
the broker must still complete its own installed acknowledgement before using
methods. The callback does not wait for methods or that acknowledgement.

Preparation/custody expiry, broker lease expiry, cancellation, stream loss,
changed Host identity and persistence failure close the lifecycle. Closure joins
owned service work outside the state mutex. No terminal lifecycle becomes ready
again. Durable notification obligations remain available for reconciliation;
loss cannot establish non-application. Busy input may retain an admitted
attachment while Trigger independently refuses new input.

The private continuing-owner core opens and retains the actual Host controller
under an explicit finite custody deadline. Individual status request cancellation
leaves that owner intact. Owner cancellation, expiry or explicit Host closure
joins its services and releases its controller without stopping the runner.
Current-object checks reject a copied Host, changed execution/epoch/runner,
stop or storage failure; observed loss is terminal. Canceling a close waiter
does not abandon cleanup. The owner core supplies resource ownership only;
it cannot implement member admission or activate a notification attachment.

A private record-source reader opens the composition-selected existing member
record directory. It resolves an explicit opaque admission reference uniquely,
retains the actual directory/file identities and exact byte revision, and rejects
missing or duplicate references, symlinks, writable sources and later substitution.
It checks current ownership before each observation and closes retained handles
when that owner ends. A separately opened source or copied owner cannot substitute
for an existing retained origin. Observed revision loss remains terminal after
matching bytes are restored.

The original record fields are validated against the pinned published
session-manager schema. Strict JSON validation precedes separating the new optional
notification input, whose fields are closed and explicit. Legacy records may
coexist without notification input. The reader has a 64KiB per-record bound and
1024 directory-entry bound; incomplete or indeterminate scans refuse. Errors
contain no record values. Filesystem checks are optimistic current observations,
not an exclusion lock against a concurrent operator writer or continuous-history
proof. Actual profile/workspace/workflow/operation admission and Source's exact
server predicate and current named selection remain required. No record-reader
method grants notification custody or allocates an attachment.

The continuing manager validates complete launch/resume and configured-record
input through the published session-manager schema module. It independently
resolves the member reference, compares runtime, profile lock, selectors and
explicit timeout, and retains the physical workspace and exact operation bytes.
Plain requests retain null station admission keys. A supplied station key must
match the selected record. Legacy doorbell and Host intent conflict.

The public, a2a-independent `manager` Go package exposes `Open`, `Prepare`,
`Attachment.Bootstrap`, `Attachment.Projection` and `Close`. `Open` retains an
actual controller and record source for an explicit finite lifetime; it starts
no native runtime. Options and projections establish no admission on their own.
The current resource-only `Open` supplies no ordinary runtime-operation admission,
so its public `Prepare` refuses before contacting Core or a broker. The selected
wire handler must retain the actual operation/controller correlation. Options,
expected JSON and copied snapshots cannot supply that private admission boundary.
The implementation below that boundary invokes the existing fixed `curator config launcher` and
`curator env resolve RUNTIME --profile PROFILE --format json` readers in the
retained workspace, without repair. Outputs and execution are bounded. The
manager consumes the selected workflow and profile identity, compares the whole
canonical fragment digest, and leaves composition fields opaque. The Launcher
retains responsibility for its complete fragment parser and composition.
Unavailable or stale Core reads refuse; no repair or metadata fallback runs.

Configure one trusted `Broker` in the optional composition. Its `Prepare` must
independently observe and retain its provider origin before Host socket
allocation. `Origin.Recheck` checks the actual manager; copying `Origin` values
is insufficient. `PreparedBroker.Recheck` checks the provider's actual custody.
`PreparedBroker.References` supplies the retained provider scope's opaque member,
connection, configuration and profile references. The connection reference may
differ from the public stable connection selector; no equality or derivation is
assumed. Those actual references stay frozen and are rechecked against the same
prepared object.
Both run again at lifecycle boundaries and before method admission, outside the
Host mutex. This is an optimistic current observation, not a filesystem writer
lock or an attestation of unobserved history.

`Bootstrap` transfers one endpoint, supplies the frozen Host identity and an
`OnEnrolled` gate, and permits no second transfer. A provider's actual live
`Enrollment` supplies its complete binding, finite lifetime and recheck. The gate
installs Host service and returns before the provider writes its installed
acknowledgement. A nil completion means the endpoint was not consumed; a nonnil
completion owns cleanup even when bootstrap returns an error. Provider return
without successful enrollment refuses. A successful completion continues under
the retained owner's lifetime, independently of the request's later cancellation.
Termination of the consumed completion also fences the Host service, even if
the enrolled view has not yet observed cancellation.
Close cancels and joins in-flight bootstrap and provider cleanup before releasing
retained record/workspace handles; it does not stop the native runner.

The provider adapter and selected manager wire-handler docking remain pending.
No default broker, provider success, native launch authority or live capability
is advertised by the generic package or these fixtures.

## Verification

Run on macOS with the project's Go toolchain and cgo available:

```sh
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 \
  -run '^TestManager|^TestMemberOperation|^TestMemberRecord|^TestContinuingOwner|^TestManagedNotification|^TestNotificationStream|^TestNotificationJournal' \
  ./internal/sessionhost ./manager
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go vet -p 1 ./internal/sessionhost
```

These tests cover local one-shot ownership, correlation, lease/cancel loss,
completion/terminal races, retained intents and closed Trigger gates, with
nearby valid controls. Descriptor checks verify actual CLOEXEC metadata only;
they do not qualify designated child inheritance or native cleanup. The existing
stream tests cover bounded exchange, framing and lost-response obligations.
Continuing-owner tests open an actual local Host against an owned inert Go
child, verify controller release and preservation of that runner, and reject
foreign Host objects beside a valid current owner. No test here establishes
real member admission, Source enrollment or productive native execution.

Record-source tests use only owned temporary files and inert children. They pair
valid retained records and legitimate legacy forms with missing/duplicate refs,
identical-byte file replacement, in-place revision changes, source substitution,
strict input failures and owner cancellation. They do not qualify a real member
record, resolved profile lock, native home or broker enrollment.

Manager tests use owned inert Core subprocesses, an explicit model of the still
unimplemented ordinary wire-handler admission, and provider models. They cover
all four emitted fragment versions, exact no-repair command arguments, complete
digest/profile/workflow mismatch, duplicate and oversized output, nonzero exit,
cancellation, missing provider, refused preparation, callback-before-return,
invalid completion, terminal origin drift and close during bootstrap. They do
not invoke real Core resolution, open a native home or enroll a live provider.
