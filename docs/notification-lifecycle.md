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

The private pre-child owner retains the same configured physical record source
without constructing a Host or runner. It has its own explicit finite lifetime;
each complete valid request consumes one preparation claim before record or
compiler refusal can occur. Successful identical requests share that claim.
Its start boundary rechecks the actual record/workspace and consumes one attempt
before returning a copied fixed invocation. Cancellation at that boundary,
conflicting request bytes, a failed preparation or a previously consumed attempt
cannot recycle it. Owner shutdown joins retained-file cleanup; completion of a
short request does not own that continuing lifetime. Existing Host-bound record
and custody methods still require a non-null actual Host owner and refuse this
pre-child source. This is resource/once-claim implementation, not an installed
ordinary wire handler, executable admission or native initialization proof; the
public resource-only admission guard remains closed.

The private ordinary-method dispatcher now connects the selected existing
method and complete published body to that pre-child owner. It retains one
configured Launcher executable and its parent directory before preparing any
request. The path must be absolute and canonical, with no symlink spelling;
the file must be regular, executable, at most 64MiB, owned by root or the
operator, and not writable by group or others. Special permission bits refuse.
Its configured lowercase SHA-256 must match. Preparation and the consumed
plan boundary recheck file/parent identity, mode, size, modification time and
the complete digest. Observed loss is terminal even after matching bytes are
restored. These are optimistic observations, not an atomic filesystem lock
or proof that an installation or policy has been admitted.

The dispatcher uses the closed configured environment parser and copies the
fixed nineteen arguments, retained workspace and baseline into a single-use
plan. An identical request shares its operation; another ID, altered bytes,
unsupported semantics, canceled take or executable drift cannot recycle a
failed claim. Optional `TMPDIR` stays optional. Paired member selectors stay
in the agreed flags, with no incoming `A2A_*` baseline entries. Owner shutdown
joins executable and member handles independently of short request lifetime.
This is private dispatch/plan preparation, with no new endpoint or public
launch entry. The returned plan remains data and is not ordinary admission.

A separate bounded diagnostic discard sink recognizes only the exact fixed
`protocol_child_environment_refused` line. All other nonempty stderr becomes
the fixed `protocol_child_diagnostic_redacted` label; raw bytes are not retained.
Its work and state are bounded per write. Every exit observation keeps phase
unverified, including statuses 64, 70 and 78 and recognized diagnostics, since
the native process can emit the same values after replacement. The runner's
`protocol-child-v1` diagnostic mode connects its actual child's separate stderr
to this fixed classifier. Empty mode preserves discarded stderr; every other
mode refuses before endpoint creation or child start. No arbitrary writer
callback receives diagnostics. The collector drains through a fixed read buffer
and retains only the constant classifier state. Native stdout remains separate.

`LauncherDiagnostics` observes the runner's actual owned PID, exit/unknown-Wait
state and independent stderr completion. Closing the read handle joins that
collector even if another writer keeps the pipe open; an interrupted read is
incomplete capture, not EOF. Neither clean EOF nor a fixed label proves which
side of exec emitted it. The ordinary operation can derive the corresponding
runner configuration from its same consumed claim, retained execution ID,
executable, nineteen arguments and copied environment. The runner endpoint and
timeout are composition-selected; no request can replace them. This remains
configuration data and does not invoke `runner.Start` or supply policy admission.
Actual selected wire entry, final execution-policy admission, direct-child
start from that admitted handler, native initialization/controller
correlation and ownership transfer still need wiring and qualification before
notification effects. The public resource-only guard remains closed.

After that independent ordinary admission, the continuing manager retains one
notification-operation claim before contacting a broker. It binds the exact
launch/resume bytes, kind and operation ID to this owner. An identical completed
request returns the same actual attachment; a pending, refused or altered request
cannot prepare another provider or allocate another pair. A closed attachment
stays the same terminal lifecycle. This in-memory reconciliation is not durable
operation recovery after manager exit and does not implement ordinary admission.
Close cancels and joins an in-flight claimed preparation before releasing its
retained record roots. Provider preparation and cleanup must honor the explicit
finite lifetime; the manager does not abandon an owned callback on waiter expiry.

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

## Protocol-child request construction

The private request compiler maps a complete validated create request to the
closed `protocol-child-v1` command: one mode and eighteen ordered flags. It
preserves null model and effort expectations and translates the validated
`sha256:` fragment digest into the command's bare hexadecimal representation.
It constructs no environment, opens no executable and grants no admission.
The final two flags are paired nullable member selectors. Their retained-record
path rechecks the same source, record revision and exact operation before and
after constructing them. Request fields or ambient identity cannot replace that
origin. This is still only the record conjunct of full runtime admission.

The separate deployment-environment reader accepts exactly `HOME`, `PATH`,
`TMPDIR`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ`, `USER`, `LOGNAME`, `SHELL` and
`CURATOR_HOME`. Every other name refuses; no foreign field is scrubbed followed
by continuation. It requires the real operator home from user-database metadata
and a nonempty configured path. It never enumerates the Host environment or
reads a credential payload. Independent installation/deployment configuration
admission remains necessary; accepted names alone do not prove it.

The initial command cannot represent native resume, a native tail, child
environment overrides, service tier, weights, credential-source selection,
request-specific prompt/sandbox/memory/goal changes or detached execution.
Such requests refuse before command construction rather than silently losing
their semantics. Launcher policy resolution, actual executable custody, the
single-use operation claim, native initialization and the real controller
correlation remain separate requirements before notification preparation.

The [selected manager handler](selected-manager-handler.md) retains ordinary
operation admission and a continuing Host owner across short requests. It uses
the configured broker interfaces without public proof constructors or new request
fields. Its acceptance adapters are inert. The productive ordinary execution
owner, concrete provider adapter, designated child transfer and native use still
require integration and qualification. This local lifecycle alone does not make
a session launchable through the Host.

## Verification

Run on macOS with the project's Go toolchain and cgo available:

```sh
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 \
  -run '^TestProtocolChild|^TestManager|^TestMemberOperation|^TestMemberRecord|^TestContinuingOwner|^TestManagedNotification|^TestNotificationStream|^TestNotificationJournal' \
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

Protocol-child request tests use the published wire vectors and literal expected
arguments. They check null/explicit selectors, every permission provenance,
otherwise valid unsupported semantics, strict request framing, bounded values
and argv separation. Retained-member tests use owned inert Host/record fixtures
to reject foreign sources, changed request bytes and observed record drift.
Environment tests pair the explicit baseline with unknown, duplicate, malformed
and synthetic identity/home/session/credential variables, singly and together.
They do not exercise a Launcher binary, resolve policy or establish complete
operation, executable or native runtime admission.
