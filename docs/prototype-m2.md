# M2: bounded Codex protocol prototype

The macOS prototype adds a Codex 0.164.0 app-server stdio adapter to the
[M1 runner and fixture host](prototype-m1.md). This is fresh Go code. It does
not import the board session daemon, its helpers, schemas or tests.

Two synthetic no-tool notices have completed through an installed Codex process
launched by Curator in native mode. Replacing the in-process host retained the
same runner instance, Curator process, Codex process and native thread. Three
startup writes and two turn submissions produced exactly five wire writes and
two separately correlated native completions. Owned stdin EOF stopped/reaped the
native process successfully. This is a disposable protocol qualification, not a
hosted member launch, process-incarnation admission or confinement claim.

The protocol follows the [official app-server guide](https://learn.chatgpt.com/docs/app-server)
and the installed release's generated schema. Schema generation was research
input only; generated upstream schemas are not vendored in the implementation.

## Pure adapter boundary

`internal/adapterprotocol.Driver` has no process, transport or credential I/O.
Construction receives opaque provider/model/effort strings, an admitted workspace,
an explicit lifecycle selection and optional metadata checkpoint. `Begin`,
`Prepare` and `Observe` return a bounded transition; `Snapshot` observes metadata.
The host owns the journal, input queue, effect sequencing and runner calls.

A transition separates outgoing actions, native input acceptance, turn outcome,
activity, optional usage and typed authority-required observations. Native command
receipt IDs and native turn IDs have different fields. An action key is stable
for the same notice ID independently of transport request counters. A successful
pipe write is neither native acceptance nor completed work.

The descriptor advertises qualified capabilities. Lifecycle create/resume/fork,
preallocated identity, next turn, busy queue, steer, interrupt, usage, graceful
exit, idle fence, native goals and human terminal support remain distinct.
Declared PTY fallback never selects another transport or grants an operation.
Unsupported Codex operations return `adapter_operation_unsupported` without
actions or metadata changes. M2 qualifies only create, idle next-turn notices,
native turn correlation, conservative activity and the input fence. It does not
qualify resume/fork, steering, interruption, usage, native goals or terminal input.

Three interface-level fixture forms protect the boundary:

| Property | Codex app-server | Claude headless stream-json | Muse MSP |
| --- | --- | --- | --- |
| Native session identity | Returned by thread/start | Preallocated | Preallocated |
| Input acceptance | Accepted turn ID | Correlated input/activity; no invented turn ID | Command receipt may precede turn ID |
| Completion | Matching completed turn | Successful result for current input/session | Matching turn and command correlation |
| Primary transport | Pipe | Pipe | Pipe |
| Declared later fallback | None | PTY | PTY |

Claude/Muse wire examples are schematic design counterexamples, not release-schema
conformance tests or native adapter implementations. Shared host persistence
fixtures exercise acceptance, command/turn identity differences, successful
completion and wrong receipt/input/session rejection. Capability fixtures exercise
explicit lifecycle, optional usage and unsupported operations. Native Claude/Muse
qualification remains future work.

## Durable delivery and recovery

Notice sequence and wire sequence are independent. A notice is durably marked
`unknown` before preparation. The entire protocol checkpoint, outgoing frame,
sequence, action key and SHA-256 payload digest commit before runner writes.
Native acceptance records `native_accepted`, optional `native_receipt`, native
session and optional turn. Only a matching successful completion changes a
synthetic notice to `applied` with outcome `completed`. This proves no mailbox
receipt, task acceptance or goal completion.

Runner protocol version 2 advertises write-receipt version 1. An authenticated
`write-receipt` query supplies a sequence and exact payload digest. It only observes
`written`, `write_unknown`, `write_missing` or `sequence_conflict`; it never reserves
a sequence or writes absent input. A restored host uses this read-only operation
for pending actions. A known written action can be retired and correlated output
replayed. Missing/unknown actions remain fenced without retries. A conflicting
receipt closes protocol processing. Native completion may resolve an uncertain
effect without opening the unknown fence or authorizing another write.

The existing private atomic journal remains bounded by 64KiB; checkpoints by
8KiB, an outbox by four 4096-byte frames, writes by 128, notices by 32, and runner
output by 32KiB. Snapshots deep-copy checkpoint and outgoing bytes. Persisted
metadata omits incoming output and partial native lines. Restore requests replay
from the last complete-frame offset. Input remains closed until the host observes
the actual retained runner tail. Gaps, rewinds, overflow, malformed evidence, lost
transport, failed/interrupted turns and EOF never silently become idle or retry.
The retained prefix is not rotated; exhaustion currently closes delivery.

The worker polls nonblocking status and reads only an existing output suffix.
Ordinary idle time therefore does not trigger a blocking-read timeout. No host
state lock spans runner I/O. An already issued write cannot be retracted by a
concurrent hold or stop; durable stop/security intent prevents subsequent writes.

## Authority and product boundaries

A native server request closes automatic input. A durable `authority-required`
observation contains a stable key, `unclassified` class and `OwnerOnly=true`.
It includes no native question, parameters or secret-dependent digest and emits
no approval response. Later notification routing can consume that observation
without parsing the native output again. Automatic notification delivery and
approval granting are not implemented in M2; operational use needs that routing.
Security STOP remains durably latched by the host.

Typed `goal.set`, `goal.clear` and the `goal-provider` capability name are reserved.
Both goal operations currently return `goal_operation_unsupported` and have no
effects or grants. Future set/clear requires a session-scoped goal-provider grant
held by the launching caller or another granted provider. Agents never set their
own goals; self-request authority and namespaces remain unresolved. A completed
turn cannot clear a goal.

The same host lifecycle can be called behind an optional dispatcher. M2 adds no
dispatcher admission, launcher payload, model catalog, home lease, account/service
provisioning, caller-scoped authentication, human lease, native-process adoption or
Linux runtime qualification. Same-UID Unix authentication still does not isolate
agents sharing that UID. The runner only signals its owned child; it does not
claim descendant process confinement. Hosted process proof and home leases are
separate prerequisites for the next slice.

## Commands and verification

After an admitted caller has started the runner with its approved non-secret plan:

```sh
.temp/prototype/bin/swarma-session-host serve \
  --socket /absolute/private/host.sock \
  --runner /absolute/private/runner.sock \
  --state /absolute/private/host-state --execution execution-id \
  --adapter codex --workspace /absolute/admitted/workspace \
  --vendor openai --model "$MODEL" --reasoning "$EFFORT" \
  --io-timeout 5s --poll-interval 25ms
```

Model/provider/effort values pass through unchanged. The Codex mode refuses
`--input-state`; readiness comes from its protocol. Existing fixture commands
remain available. `enqueue`, `hold`, `release`, `status` and `stop` retain the
[M1 control envelope](prototype-m1.md#commands). Status adds bounded protocol
metadata and separately observable native receipts; it includes no transcript.

```sh
GOMAXPROCS=1 go test -p 1 -race -count=1 ./internal/adapterprotocol ./internal/codexadapter ./internal/sessionhost ./internal/runner
GOMAXPROCS=1 go vet -p 1 ./...
```

Tests cover ordered handshake, opaque selections, acceptance/completion separation,
unsupported operations, approval without response, identity mismatch, bounds,
corrupt/partial checkpoint recovery, retained-tail catchup, known-idle replacement,
and written/missing/conflicting outbox recovery with real synthetic children.
Two isolated control plants intentionally resend missing input and infer completion
from acceptance; each fails the specific negative assertion. Source is restored
after those controls. Existing fixture/security/lifecycle tests remain applicable.

Native qualification needs the installed Codex 0.164.0 protocol and an admitted
Curator profile. Curator retains environment/credential custody; no auth files,
keys, tokens, parent agent identities or raw protocol logs enter the product
journal. Local protocol evidence does not substitute for hosted module CI or
production admission.
