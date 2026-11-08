# Proposed process-provider control protocol

Status: isolated Host-side candidate, disabled by absence of composition. The
package has an internal inert broker docking in `internal/sessionhost`, with no
launcher, broker registration, live service or default adapter. It does not
implement or qualify an a2a provider. The source owner
retains protocol approval and implementation custody.

The accepted [source decision](https://github.com/relux-works/a2a-swarm/issues/132#issuecomment-6705562655)
requires a provider process using a2a's admitted immutable image and one inherited
Unix STREAM endpoint, a closed versioned protocol, callback before ACK, and a
single locked enrollment write. `a2a host provide` is a working command name.
**Every frame name, encoding choice and additional DTO field below is proposed
for coordination, not an existing a2a API.** No external Go `internal` package is
imported. This does not extend the closed notification method wire in #175.

## Transport and grammar

`NewClient(ctx, conn, custodyExpiry)` consumes an already transferred
`*net.UnixConn`, including on refusal. It verifies AF_UNIX STREAM shape; socket
shape does not authenticate its peer. It creates no pair, listener, descriptor,
process, path dial or connection origin. The fake process test inherits precisely
one protocol endpoint through `ExtraFiles[0]` (fd 3); standard descriptors carry
diagnostics only. A live descriptor number/argv convention remains source-owned.

Each frame is `uint32` big-endian payload length followed by that many UTF-8 JSON
bytes. Length is **1..32768**, excluding the four-byte header. Reads consume
exactly one frame without buffering the following bytes. JSON uses the exact
case-sensitive fields below, forbids unknown/missing fields, duplicate keys
(including escaped aliases), nulls, arrays and trailing values, and has a maximum
nesting depth of eight. Optional means omitted, never null.

Envelope: `{"version":1,"id":1,"type":"hello"}`; optional `body` is present only
for the five body-bearing frame types below. `version` is a JSON `uint16`, pinned
to **1** in the first exchange and every later frame. `id` is a nonzero JSON
`uint32`, increasing once per Host operation without wrap. Every response and
callback uses that operation's exact ID. There is one outstanding operation.

| Host sends | Provider sends | Body |
| --- | --- | --- |
| `hello` | `hello_ok` | none |
| `prepare` | `prepared` | PrepareRequest / References |
| `recheck` | `rechecked` | none |
| `bootstrap` | `recheck_host` | BootstrapRequest / none |
| `host_checked` | `enrollment` | none / Enrollment |
| `installed` | `ack` | none |
| `close` | `closed` | none |
| any outstanding request | `refused` | Refusal |

The provider must validate the very first `hello` and refuse a mismatched version
before any preparation/enrollment effect. Host sends no `prepare` before the
matching `hello_ok`. Unknown versions, frames, IDs or unsolicited responses end
the stream. A validated refusal is terminal even when immediately followed by
EOF; its closed reason survives. EOF, timeout, truncation or cancellation never
prove that an earlier operation had no effect.

## DTOs and bounds

The Go structs in `contract.go` are the exact proposed schema. Every field is
required, including the two explicitly empty Bootstrap binding fields. Bounds
count UTF-8 bytes. References are nonempty valid UTF-8, exclude control characters
and the replacement character, and carry no authority by their shape.

| DTO | Required fields |
| --- | --- |
| PrepareRequest | `connection` (128), `profile_lock_ref` (512), `workflow_reference` (scalar), `workflow_sha256` (digest) |
| References | `member_ref` (512), `connection_ref` (128), `configuration_ref` (512), `profile_lock_ref` (512) |
| BootstrapRequest | `binding` (prefix), `attachment_ref` (128), `call_timeout_ms` (integer 1..60000) |
| Enrollment | `binding` (full), `attachment_ref` (128), `source_session`, `consumer_ref`, `lease_holder`, `lease_host`, `process_birth` (each 128), `lease_pid` (positive int64), `lease_expires_at` (canonical RFC3339Nano UTC with `Z`, 20..30 bytes) |
| Refusal | `reason` (one closed value below) |

Binding has all twelve fields, with the accepted names: `execution` (scalar),
`controller_epoch` (canonical nonzero decimal uint64 string), `runner_instance`
(scalar), `native_thread` (scalar), `member_ref` (512), `connection_ref` (128),
`configuration_ref` (512), `profile_lock_ref` (512), `consumer_generation` (128),
`enrollment_ref` (512), `workflow_reference` (scalar), `workflow_sha256` (digest).
Scalar is 1..128 ASCII letters, digits, `_`, `-` or `.`. Digest is exactly 64
lowercase hexadecimal characters. The Bootstrap prefix has empty strings for
`consumer_generation` and `enrollment_ref`; a2a issues both during enrollment.
The full Enrollment binding requires both nonempty.

The client compares all ten supplied binding fields, original attachment,
frozen References, and the selected workflow correlation. Opaque connection
reference and public connection selector may differ. The enrollment callback
must independently compare **all twelve fields**, source session, consumer,
exact lease and process birth with retained live admitted origins. Provider
metadata is never a replacement for those objects. It must check current lease
and physical origin custody; copying request values is insufficient.

Closed refusal reasons: `unsupported-version`, `admission-unavailable`,
`origin-unavailable`, `binding-conflict`, `stale-binding`, `lease-unavailable`,
`consumer-conflict`, `enrollment-unverified`, `operation-expired`,
`observation-unavailable`. Other failures return fixed diagnostic errors without
received text. A refusal applies only to its operation, never an earlier unknown
attempt. This package sends no notification attempts or mailbox receipts.

## State and finite ownership

`hello → prepare → [recheck]* → bootstrap → recheck_host → host_checked →
enrollment → recheck → rechecked → installed → ack → [recheck]* → close → closed`.
Close is also allowed before Bootstrap; successful Close is idempotent locally.
Prepare and Bootstrap each happen at most once. No resend, recovery attachment,
renewal, reconnect, selector fallback or legacy handback exists here.

On `recheck_host`, Host's retained manager-only callback must return successfully
before `host_checked`. On `enrollment`, the enrollment callback handler first
performs a fresh provider `recheck`/`rechecked` exchange under the same Bootstrap
ID. The provider must recheck its exact retained live object and frozen binding;
this does not call the composite Host recheck or acquire the consumer lock again.
Local binding comparisons and Host's retained verification/installation callback
then run and must return successfully before `installed`. The provider
must not send ACK until it receives `installed`. No callback may wait for ACK,
methods, Done, or recursively call this client. Short state/control locks must be
released before callbacks and I/O; the source retains sole-consumer ownership.

A continuously owned reader rejects duplicate/early ACK, including a duplicate
arriving while idle after the first ACK. The first valid ACK can be observed
before a later duplicate arrives: Bootstrap success is subject to the continuing
`Done`/`Err` lifetime, not proof of future stream integrity. A future composition
must fence any installed service on Done. A callback already ran when ACK is
missing; that loss stays unresolved and cannot justify another pair.

Every operation, including gate wait and callbacks, has one absolute deadline:
earliest caller deadline, retained custody expiry, known lease expiry and 60 s.
On receipt, a syntactically valid enrollment expiry immediately narrows the
pending wait and continuing lifetime, including the nested source recheck. An
already expired lease is refused before that recheck. This conservative bound
does not admit enrollment: the fresh source recheck and retained verification
must still succeed before installation.
Successful short-caller cancellation does not revoke the continuing owner.
Lease/custody expiry closes the stream; it does not stop a runner or erase a
persisted fence. Close requests `closed` when idle; during an outstanding
operation it cancels immediately by closing the stream. It joins owned reads and
callbacks within five seconds or the earlier caller deadline. Callbacks must
honor their context. An uncooperative callback returns `ErrCleanup` at the bound;
the package cannot forcibly terminate Go code and does not claim a clean join.

## Decisions needed before composition

1. Source approval of this proposed grammar, exact provider image/head, command
   flags and inherited descriptor convention. There is no live compatibility pin.
2. Source implementation of the independently admitted server/root/profile
   predicate, source-issued generation, durable single locked v1/v2 enrollment,
   required refusals and original-key UNKNOWN reconciliation. No mailbox writes;
   receipts remain explicit inbox/ack operations. This client proves none of it.
3. Exact scheduling of Prepare versus allocation/transfer: the earlier draft
   put Prepare before pair allocation, while the process decision requires its
   inherited stream for Prepare. This candidate accepts an existing stream and
   creates no second channel; source must settle the production ordering.
4. The post-ACK sharing or phase transition between provider controls and #175's
   method traffic on this **same** stream. This reader owns control framing
   throughout and must not run concurrently with a method reader. There is no
   proposed method frame, envelope extension or stream handoff in this package.
5. The retained live enrollment/completion mapping through the registered
   `curator-session-manager/1` seam. Host now exercises that mapping with a private
   inert broker and independently retained fixture enrollment. It preserves the
   manager-only callbacks, nested provider recheck, callback-before-ACK ordering
   and continuing Done fence. The broker retains the allocated method endpoint
   only for cleanup: it neither transports it to a provider nor reads it. No
   second control channel, method framing or reader handoff is supplied. Real
   origin verification, admission and method transport remain unimplemented.
   Fresh independent review and source-owner approval remain required before
   live composition or activation.
