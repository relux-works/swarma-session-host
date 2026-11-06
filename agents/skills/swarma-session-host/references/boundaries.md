# Trust and refusal boundaries

## Receipts and recovery

- Enqueue persists `queued` intent before acceptance. Exact ID/intent retry
  returns the existing record; changed intent returns `request_conflict`.
  Before fixture I/O the Host persists `unknown`; only a complete matching
  adapter acknowledgement sets `applied`. In protocol mode native acceptance
  and successful correlated completion are separate facts. None of these is an
  a2a inbox receipt, task-success or goal-completion verdict. [C08](evidence.md#c08)
- Missing acknowledgement, output gaps/loss, interrupted or failed protocol
  evidence keep delivery closed. Replacement queries existing write receipts;
  missing/unknown writes are not resubmitted. Preserve the same runner, execution,
  state and mode, choose a new host socket after an abrupt exit, and read the
  returned epoch. Do not infer absence from a timeout or delete uncertainty.
  [C09](evidence.md#c09)
- Missing initialized state, pending transaction markers, corrupt snapshots,
  changed plans and duplicate Host ownership refuse. Release cannot reopen an
  unknown fence. Security STOP is durable and later idle, release or replacement
  cannot clear it. Hold or a new observation cannot retract an issued write.
  [C09](evidence.md#c09) [C10](evidence.md#c10)
- Keep the bounded output and journal contracts: 32 retained notices, 128 runner
  writes, 64KiB Host state, 8KiB protocol checkpoint and at most four 4096-byte
  outgoing actions. Runner control protocol 4 retains 69,633 output bytes with
  absolute offsets and 4096-byte reads. Only durable complete-frame checkpoint
  consumption frees bytes; full backlog applies pipe backpressure. Cumulative
  output may exceed the retained bound. [C14](evidence.md#c14)

## Authority and process scope

Private Unix endpoints check peer UID; they do not distinguish agents sharing
that UID or provide production caller capabilities. Private modes do not make
untrusted ancestor directories safe. Supply only admitted nonsecret references
and plans; the closed state schema is not a general credential detector.
[C07](evidence.md#c07) [C20](evidence.md#c20)

The Codex adapter reports a native server request as owner-only,
`authority-required`/unclassified attention, closes input and emits no approval
answer or raw question. It supports create and idle next-turn notices, with
correlated native turn identity, activity and idle fencing. Resume/fork,
steering, interrupt, usage, native goals, terminal input and PTY fallback are
not qualified Codex operations. Goal names confer no grants.
[C11](evidence.md#c11) [C13](evidence.md#c13)

Host Close releases its controller without stopping the child. Explicit stop
persists stop intent and asks the runner; the runner can signal only its
original still-owned unreaped direct child. Default `none` is EOF-first;
opt-in `term` shares one cooperative attempt and bounded grace/reap/drain
phases across retries. `stopped` does not prove successful work, normal exit,
complete drain or descendant absence. Inspect exit/output/overflow separately.
[C15](evidence.md#c15)

Runner identify is a separate read-only ownership snapshot; even an exited
unreaped child can have verified ownership. Its qualification client is
unexported and has no command caller. It establishes neither current liveness
nor hosted admission; Linux identify peer proof is unsupported. Never replace
the missing authoritative external execution record or fresh live check with
legacy status, a random runner instance or that snapshot. [C22](evidence.md#c22)

## Notification and unavailable integration

The `curator-host-notification/1` codec handles `observe_binding`, `trigger`
and `lookup_attempt`; these are notification methods, not Host CLI operations.
Frames are strict UTF-8 JSON plus LF with a 65,536-byte inclusive limit, exact
keys/types and response correlation. Invalid or incomplete responses, including
EOF, remain unresolved; close the attachment and retain possible accepted
obligations. [C16](evidence.md#c16)

The journal retains one binding and same-attempt correlation. Trigger checks
current binding and input/fence/security/capacity gates atomically with intent;
lookup/retry does not create a new attempt. Stream loss fences input and cannot
rearm the lifecycle. The private manager checks owner, live enrollment, attachment,
all twelve binding fields and lease before installation; it allocates one
CLOEXEC socketpair and transfers the source endpoint once. An attachment's local
`ready` state is not input readiness or production admission.
[C17](evidence.md#c17) [C18](evidence.md#c18)

G07, grant consumption, live runtime and root custody are unavailable for this
workflow. There is no supported grant-consumption or root-custody operation in
these entrypoints. Private manager/broker check implementations, designated
child transfer and live notification delivery remain unimplemented. M3-S checks
injected synthetic record/lease/process evidence only; it neither creates a
launcher plugin nor acquires/binds/releases a real lease. Do not route around
a refusal through the unadmitted fixture CLI. [C00](evidence.md#c00)
[C21](evidence.md#c21)
