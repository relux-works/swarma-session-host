# M1: fixture host and runner

## Scope

M1 provides a fresh Go runner, host skeleton and two synthetic adapters. It
launches no agent harness, contacts no model provider and implements no Curator
session-manager capability. It does not import or copy board-daemon code.
macOS/arm64 is the first validation target; Linux has a peer-credential backend
but is not qualified by macOS results. No mobile platform is supported.

M2 qualifies Codex against the installed protocol. M3 exercises `curator run`
with the member's admitted profile, explicit yolo, owned tmux resources and a
real final liveness check, only after the versioned plan/process-proof seam is
accepted. Claude Code is the next adapter after Codex. Existing launcher home
leases and manager admission remain required. M1 neither bypasses those gates
nor claims that fixture model forwarding satisfies them.

## Responsibilities and authority

- The runner owns exactly one child, stdin/stdout, a public random incarnation
  identifier, bounded output and a current controller connection/epoch. A host
  disconnect does not stop the child. Controller claim serializes with writes
  and stop; an active controller cannot be displaced by a larger epoch. A new
  same-user controller can claim a higher epoch after the old one releases.
- The host holds one nonblocking process lock per state directory. It owns a
  bounded snapshot journal of notice references, intent digests, sequence numbers
  and receipts. It uses a runner connection rather than owning child stdio.
- Adapter implementations encode a notice and observe the matching synthetic
  acknowledgement. JSON and tab-separated line adapters plug into the same
  interface; core contains no Codex or other harness-specific types.
- Adapter input observations are `idle`, `busy`, `draft`, `approval`, `unknown`
  or `security-stop`. Delivery requires explicit idle evidence as well as the
  manual fence. Absent/unrecognized evidence becomes unknown. Security STOP is
  durably latched; later idle, release and host replacement cannot clear it.
  These are fixture observations, not a real harness readiness parser. A changed
  observation cannot retract a write that has already been issued.
- Opaque vendor/model/reasoning strings are passed to the adapter byte-for-byte.
  The scalar grammar only bounds shape: `[A-Za-z0-9._:/@+-]{1,128}`. There is no
  catalog, ranking, model fallback or assertion of provider support.

Private directories are 0700; sockets, lock and state files are 0600. Both ends
check the kernel peer UID. This authenticates an OS user, **not agents sharing
that UID**. Controller connection ownership is a fixture invariant, not a
production caller capability. Ancestors of configured private directories must
be trusted; this prototype is not an adversarial-filesystem boundary. No new
account, credential, profile or daemon is provisioned.

## Durable state and delivery

`state.json` has a closed version-1 shape: execution, public runner instance,
adapter name, selection, controller epoch, sequence, fence, stop intent, retained
records, input observation and the security-stop latch. Records contain ID/reference, intent digest, sequence and
`queued`, `unknown` or `applied`. They contain no mailbox body, arbitrary prompt,
child argv/environment or encoded adapter stream. This is a bounded snapshot
journal, not a complete append-only transition audit or a secret classifier.

Updates first sync a private `state.pending` marker, then use a private temporary
file, file sync, rename and directory sync, then remove/sync the marker. A crash
with that marker retained refuses startup as an unknown transaction; M1 does
not automatically repair it.
Read, schema, permission or sync errors refuse effects. Missing state after an
initialized lock exists is lost evidence, never a fresh session. A changed
stored execution, adapter or selection refuses startup. A different runner
instance cannot be adopted as the previous execution.

Enqueue persists intent before acceptance. An exact ID/intent retry returns the
original record; changed intent under that ID conflicts. Before writing, the
host persists `unknown` as its in-flight marker, then releases its state lock
for adapter/runner I/O. Only a complete matching adapter acknowledgement sets
`applied`. A pipe write is insufficient. Missing/malformed acknowledgement or
failure retains unknown state and closes delivery; restart and explicit fence
release cannot replay it. Automatic unknown reconciliation is not implemented.

A held fence can retain bounded notices without writing. An operation already
marked in flight may finish while a hold is requested; holding does not retract
an issued effect. Unrelated status remains available during runner I/O. Explicit
stop persists stop intent before signalling; it cannot be undone by release,
enqueue or host restart. Queued receipts remain retained after stop.

The runner stores each write sequence/digest before touching stdin. Exact retry
returns its recorded result without another write; changed bytes under the same
sequence conflict. Partial/failed writes close subsequent writes. Output loss
or overflow is explicit and never a successful acknowledgement.

Shutdown closes admission under the same lock used to register accepted
connections. A connection accepted before closure but registered afterwards is
closed without becoming a controller; a claim after shutdown is refused.

## Process model and limits

The binaries run in foreground; an outer owner may provide separate tmux
resources. M1 does not install launchd/systemd services or claim survival of
service-manager replacement. The automated smoke proves a narrower fact: after
one owned host process receives SIGKILL, the independent runner retains the
same synthetic child and accepts a replacement controller.

The runner never signals a PID supplied by a request or reconstructed from
state. It serializes its own nonblocking `wait4` reaper with signals: until reap,
the PID remains assigned to that child; after reap, stop is inert. This avoids
relying on Darwin's concurrent `os.Process.Wait`/`Signal` behavior. Reaping polls
every 10ms in this fixture implementation. Actual process birth proof and
whole-descendant termination are not implemented. Fixture children spawn no
descendants; root-child exit is not account-wide absence.

| Resource | Bound / behavior |
| --- | --- |
| IPC frame | 64KiB; closed JSON and bounded reads |
| Runner write | 4096 bytes; 128 retained sequences per runner |
| Runner stdout | 32KiB retained; overflow closes writes, reports unavailable; no silent replay-tail truncation |
| Host records | 32 retained records, including completed/unknown receipts; no deletion to admit more |
| State snapshot | 64KiB; refuse before acceptance when it cannot be written |
| Connections | 8 per endpoint; excess connections close |
| I/O | Explicit positive timeout, at most 1 minute; control operations require deadlines |
| Stop | Close stdin; after the explicit timeout, signal only the owned unreaped child; retain unknown if exit is unconfirmed |

Child environment is explicit, never inherited. Fixture stderr is discarded,
not interpreted or stored. Runner output is in memory only and must remain
within the cap; a runner crash loses this evidence, so host delivery stays
closed. No standing-session lifetime, unattended recovery scheduler, human
input writer, permission auto-answer or provider security retry is implemented.
Unknown real harness states must not be mapped to fixture readiness.

## Commands

Build using the [README](../README.md#build-and-verify). Every socket path must
be absolute, under a private directory, and at most 100 bytes for macOS. The
prototype refuses occupied paths and never removes another endpoint. After an
abrupt host exit, choose a new host socket for the new incarnation; retain the
same state directory and runner socket. Cleanup belongs to the fixture owner.

`session-fixture -protocol json|line` reads synthetic stdin and acknowledges on
stdout. `swarma-session-runner` reads this non-secret plan from stdin:

```json
{"binary":"/absolute/path/session-fixture","args":["-protocol","json"],"cwd":"/absolute/private/directory","env":[]}
```

```sh
swarma-session-runner -socket /private/path/runner.sock \
  -execution fixture-execution -io-timeout 1s < fixture-plan.json
swarma-session-host serve -socket /private/path/host-1.sock \
  -runner /private/path/runner.sock -state /private/path/state \
  -execution fixture-execution -adapter json-fixture \
  -vendor future-vendor -model unlisted-model -reasoning new-effort \
  -input-state idle -io-timeout 2s
swarma-session-host request -socket /private/path/host-1.sock \
  -timeout 2s < fixture-request.json
```

Use only non-secret fixture plans/references. A request is one bounded JSON
object. Read-only `status` needs no session selector; mutations require the
current execution and epoch, for example:

```json
{"op":"enqueue","execution":"fixture-execution","epoch":1,"notice":{"id":"one","reference":"fixture/one"}}
```

Operations are `status`, `enqueue`, `hold`, `release` and `stop`. Responses carry
closed codes and a bounded state snapshot. JSON response receipt is not an a2a
inbox receipt, a processed real agent turn or a goal-completion verdict.

The CLI requires `-input-state idle` to permit fixture delivery; its absent
default is unknown. Other fixture states keep input ineligible, and
`security-stop` latches. The state is fixed for that CLI incarnation. Embedded
fixture adapters can update observations and call `ObserveInput` to wake queued
work. Real harness observation must be separately qualified.

## Evidence required

Run package tests and race detection plus both process-smoke protocols. Tests
pair valid controls with stale/competing epochs, wrong execution, changed intent,
unknown acknowledgement, corrupt/truncated/missing state, duplicate ownership,
queue/output pressure, explicit-stop persistence, and full acknowledgement
selection binding. A fabricated noncredential canary checks the wire-to-state
boundary; it does not demonstrate general credential detection. Cross-UID
rejection and actual kernel PID reuse are not simulated into passing claims.

`TestDispatchCrashBoundaries` uses real owned-host SIGKILL at three deterministic
barriers: after the in-flight record commits, after the runner accepts bytes but
before the host receives the write reply, and after a complete matching fixture
ack but before receipt commit. Test-only adapter wrappers and a Unix proxy
provide these barriers without production fault flags. Both encodings must keep
the same runner/child, exact bounded write count, retained unknown record and
refused replay/release. The already-applied path is the positive control.
`TestShutdownRejectsLateRegistration` pauses a real accepted connection before
registration, queues its claim, closes the runner and resumes registration with
the client left open; the ordinary live registration is its paired control.
Eligibility negatives observe a bounded live dispatch window and drain the host
before checking zero writes; idle supplies the successful control.

Bounded negative control plants must demonstrate that the stale-write and
changed-selection checks detect their named violations. A first failing smoke
protocol must not prevent the second protocol from executing. Keep commands,
exit codes and reports tied to the exact candidate; fixture success is not real
Codex, Curator, tmux deployment or production security qualification.
