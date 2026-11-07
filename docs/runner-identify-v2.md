# Linux runner-identify/2 implementation

This successor implements owner-approved OI-1 against exact contract revision 3 SHA-256
`c4e9b6c1115d90de68a9dd49ea7ddf584ec7c501532f994035ca5c80f8c32bf0`.
Independent technical acceptance of that text is separate from required
independent review of this implementation. The contract archive's pending OI-1
status predates the explicit owner decision. Source custody and signed landing
remain separate gates. No production route is enabled here.

## Startup selection

`runner.Config.IdentifyV2` is a bool with zero value false. The runner executable
exposes exactly `--identify-v2`, default false. The caller must select true before
Start. Start copies the boolean and it is immutable for that lifetime. No Plan
JSON member, environment variable, workload argv, wire schema, capability result
or process-global cache can enable it. Launcher configuration mapping remains
its owner's responsibility; this patch invents no launcher key.

False uses ordinary `os.StartProcess`, performs no v2 preflight or self-scope
acquisition and requests no identity-v2 CLONE_PIDFD capture. The Go runtime may
use its own pidfds. V2 evidence is unsupported: valid offers retain the existing
mismatch, busy, closed, wait-unknown and reaped precedence, then refuse
`unsupported_platform`. Offers do not change selection or acquire evidence.
Linux v1 still refuses `unsupported_platform`; Darwin v1 is unchanged. Missing
pidfs, ioctls or `/bin/sleep` cannot disable this ordinary path.

True selects the qualified Linux/amd64 ABI. Other targets fail Start before
Listen, pipes, probe child or workload creation. After side-effect-free plan and
mode validation, selected Linux/amd64 runs one preflight before Listen or
workload pipes. Failure returns `child_start_failed`, no Server, no endpoint,
no ready record and no workload. CLI preserves `fixture_runner_start_failed`,
exit 1. No degraded selected endpoint, retry or fallback is allowed. Successful
preflight permits Listen, instance/pipes, the sole original workload spawn and
synchronous actual capture, then existing collectors/acceptor/reaper. An occupied
endpoint still prevents any workload spawn; the inert probe may already have
completed. Failed actual workload capture remains a separate running-runner
refusal and never causes another workload attempt.

## Fixed preflight and ownership

One entry-relative monotonic `preflightObservationTimeout` of exactly 1000ms
covers self, private connected socketpair credentials/round-trip, spawn/capture,
live, termination, zombie, sole Wait and reaped observations. Each stage checks
the same absolute deadline before and after; the zombie loop never resets it.
Socket IO shares that deadline and lifecycle polls use zero timeout. Expiry is
permanent failure even if mandatory cleanup completes later.

The inert program is fixed `/bin/sleep`, argv `["sleep", "5"]`, empty environment,
all standard descriptors on `/dev/null`, inherited runner credentials/scope,
no workload cwd/argv/env, and atomic PidFD initialized to -1. No shell or PATH
lookup participates. Qualification records the resolved probe executable and
bytes; the probe provides no separate workload-image proof.

The scoped preflight owner establishes cleanup immediately after successful
spawn, including before post-spawn expiry or partial capture. It validates the
full pidfs/GET_INFO/namespace/CLOEXEC evidence, live empty poll and fdinfo PID,
SIGKILLs the owned process handle, observes POLLIN only and original fdinfo PID
before reaping, consumes its sole Wait, then requires POLLIN|POLLHUP, fdinfo -1
and the unchanged inode. A consumed or uncertain Wait is never retried.
Every failure with an unconsumed child wait performs owned termination and that
sole Wait before return. Scoped fds close once. Cleanup may delay an error return
beyond 1000ms; this is an observation deadline, not a realtime syscall bound.
Per-call unexported fault-injection hooks have no runtime configuration carrier.

## Capture, wire and remaining gates

Support is determined by observed capabilities, never uname parsing. This
component implements only Linux/amd64's qualified 64-bit raw syscall ABI:
CLONE_PIDFD, genuine PID_FS_MAGIC and full-width nonzero inode, version-0 64-byte
PIDFD_GET_INFO with PID|CREDS, NSFS PID/user namespace pins, connected exact-size
SO_PEERCRED, and required live/zombie/reaped behavior. Other targets need separate
implementation and runtime qualification. Kernel release is provenance only.

Actual child capture remains at the original atomic spawn before goroutines.
It never reacquires a child pidfd by numeric PID, repairs capture, substitutes a
child or synthesizes Darwin birth. Effective UID comes from GET_INFO, not pidfs
st_uid; PID/TGID/parent/euid are validated around namespace capture. Namespace and
credential changing attributes, including AmbientCaps, are rejected on the
selected helper. Procfs/namespace pins, CLOEXEC and sole serialized workload
reaper custody are retained, including partial-capture failure. Snapshots copy
bounded memory under TryLock and perform no syscall or delayed reap.

The closed v1/v2 codecs, 1024-byte LF frames, result/reason precedence, first-frame
schema dispatch, no pipelined control, handler/connection bounds, exchange
deadlines, separate typed incarnation and no downgrade remain unchanged. See
`runner-identify-v2.schema.json` and `runner-identify.md` for wire shape; lexical
duplicate-key and Unicode rules also live in the Go codec.

Qualification requires actual capabilities, not skips:

```sh
GOMAXPROCS=2 SESSION_HOST_REQUIRE_LINUX_PIDFS=1 go test -p 2 -race -tags t4fixture -count=1 -json ./...
```

Tests cover selected/default behavior, pre-Listen refusal, no endpoint removal,
validation order, no per-offer probe, immutable selection, stage failure/expiry,
partial capture cleanup, no Wait retry on uncertainty, cumulative deadline and
late Wait rejection, alongside existing kernel/codec/controller/receipt tests.
Binary qualification additionally blocks pidfd_open to compare default-path
availability with selected failure and records endpoint creation events.

Ownership is not liveness, image proof, custody, per-agent isolation or native
admission. Connected runner incarnation/boot/scope/image, fresh post-decode child
verification within the original exchange deadline, image/backend qualification,
admitted records and real Source factory/installed ACK remain external. Snapshot
restore/cloning can preserve the boot/identity tuple and is not detected.
Additional-host/fleet and upstream-minimum-kernel runtime are not qualified. Shared UID
remains a single trust unit. Independent exact-head review and normal signed
landing are required; no real provider, canary, installed service or cutover is
authorized by this candidate.
