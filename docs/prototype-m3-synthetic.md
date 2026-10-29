# M3-S: synthetic host admission

The internal admission seam exercises the host-owned part of the
[hosted-launch dependency contract](m3-hosted-dependencies.md). It is not a
launcher plugin, production Curator observer or real home-lease integration.
No new command launches a member, provisions a profile or calls Curator.

## Evidence and authority

`internal/admission` version 1 defines an internal synthetic record and lease
observation. These are typed injected values, not the launcher's published
session record or observation v2. The immutable non-secret projection binds
operation/registration, session/execution/generation, admitted-plan digest,
profile lock/fragment, environment, workspace/home, transport/permission
projection, opaque selection, adapter/release/native-session,
runner instance/socket, and separately labelled runner/harness process tuples.
Here `runtime` names the synthetic adapter; future launcher integration must
explicitly map its canonical runtime identifiers. No model catalog is added.

The lease observation binds its ID/request ID/revision, home/profile/lock,
registration/session/generation and the harness `{pid, started_at}`. Only
`bound` qualifies; prepared, released, broken, unknown and conflicting values
refuse. The full process tuple stays in the record. Lifecycle `started` is
independent of activity, operation phases, successful work and release evidence.

An embedding caller opts in with `sessionhost.Config.Admission`: a frozen
expected snapshot, read-only record/lease provider and process observer. The
providers must honor the supplied context deadline. They have no acquire,
bind, release, break, launch or signal methods. The caller establishes the
admitted scope; this seam creates no caller capability or production authority.
Opaque configuration and claimed release values are not independent proof of
actual launcher admission, script contents or harness version.

The host validates its configuration against execution/socket/adapter/selection,
verifies current evidence after the runner claim, and commits metadata before
starting dispatch. Every automatic write revalidates evidence; fixture input
also checks again after Encode. Protocol-native session observations must match
the expected identity before being persisted. Current kernel tuples must match
exactly; record and lease revisions cannot decrease, and immutable bindings
cannot change even under higher revisions. Controller epochs do not replace
execution identity or lease authority.

## Kernel reader and limits

The Darwin/cgo reader uses `proc_pidinfo(PROC_PIDTBSDINFO)` birth seconds and
microseconds plus effective UID, `kern.bootsessionuuid`, and read-only
`csops(CS_OPS_CDHASH)`. The latter ABI is documented in
[Apple's XNU codesign header](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/codesign.h).
Birth timestamps use UTC RFC 3339 with exactly six fractional digits. The
reader checks birth/UID and image twice around its observations and refuses
zombies, unavailable reads, malformed identity and canceled contexts. The
owned Go arm64 test image provides the valid ad-hoc cdhash control.

An unsigned image or unreadable cdhash is unavailable, never a matching image.
A script-hosted harness yields its interpreter's cdhash; this does not identify
the script or prove its contents. A cdhash identifies an image, not its trust
or semantic release. Separate reads are not an atomic lifetime guarantee or
protection against arbitrary exec races. No cross-UID isolation, genuine kernel
PID-reuse exercise or descendant confinement is claimed. Without Darwin/cgo,
observation reports `process_observer_unsupported`; no alternate identity is
silently substituted. The public reader performs no process discovery; this
qualification supplies only owned synthetic PIDs.

## Persistence and failure

Admitted stores use host-state version 2 with the verified metadata snapshot;
existing unadmitted M1/M2 fixture stores stay version 1. An initialized store
cannot add/remove admission mode on reopen. Version-1 binaries reject version 2.
An initial failed marker is committed before runner contact: a crash during
verification cannot become a successful admission on recovery.

Observation loss closes delivery, retains the last verified snapshot with a
failed latch, and preserves queued/unknown receipts. Idle and ordinary fence
release cannot clear it, and reopen refuses rather than silently trusting old
evidence. The existing host can still inspect status and stop its directly owned
runner child. Recovery of a failed admission requires a later explicit contract;
this slice does not clear the latch. Existing corrupt/missing/pending/private-file
checks remain in force. Native stdout, prompts, argv, environment and credential
values are not added to the admission snapshot or diagnostic errors.

Valid host replacement rechecks the same binding and process tuples. It does
not acquire a new lease, spawn another child or resend an uncertain write.
Root-child exit, host death and detach never release the home. The runner's
owned unreaped-child signal discipline remains the only stop authority.

## Qualification boundary

Synthetic tests pair valid delivery with process/lease/identity mismatches,
revision rollback, observer errors/timeouts, revoked leases after Encode,
protocol-native identity conflicts and journal corruption. Actual owned-host
SIGKILL before verification preserves the failed initialization marker;
SIGKILL at in-flight, acknowledgement and applied boundaries retains the same
runner/child and uncertainty. Bounded guard-removal controls check that process
comparison and immediate pre-write lease revalidation detect their violations.

Native-session conflicts use the same durable failure path as observation loss.
Input remains unknown through idle refresh after failure; a security-stop
observation still sets the separate sticky security-stop latch. The host pair
matrix checks each identity class at initial admission with exact refusal and
zero writes, alongside valid delivery and higher-revision reconnect controls.
A test runner peer counts actual signals to its one owned child: observation
loss sends none, while explicit owned stop sends one. This tests host routing;
the production runner has its own stop qualification and unchanged wire format.

Real-reader controls use an owned live child, altered expected UID/birth/CDHASH,
a confirmed zombie and a reaped child. `/bin/ps -p PID -o stat=` supplies the
non-reaping zombie barrier. Apple Silicon cannot execute a live unsigned arm64
image; an actual failing `csops` operation instead checks unavailable evidence,
with valid CDHASH reads before and after it. These controls do not prove atomic
identity observations or arbitrary PID reuse safety.

Run focused tests with:

```sh
CGO_ENABLED=1 GOTOOLCHAIN=local GOWORK=off go test -count=1 ./internal/admission
CGO_ENABLED=1 GOTOOLCHAIN=local GOWORK=off go test -count=1 -run 'TestAdmission|TestAdmitted' ./internal/sessionhost
```

Use the README module vet/race commands for full validation. The fixture CLI
remains an explicitly unadmitted prototype, not a fallback from hosted refusal.
Real M3 still needs the versioned launcher record/observation, released lease
API and production observer/bind path, then separate live macOS qualification
through Curator with its admitted profile, permissions, tmux and liveness proof.
