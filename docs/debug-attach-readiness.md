# Debug attach — host-private part B readiness

This note records what the session-host side of the
[debug attach contract 0.2.1](debug-attach-contract.md) implements as unit B
(C22 row 1a) and what it deliberately does not. Everything here is private to
`internal/sessionhost`: there is no launcher schema member, socket, CLI flag,
capability advertisement, native text route or alarm sink. Passing the B
fixtures is standalone host evidence, not a native, launcher or rollout
qualification (units C–G of the contract).

## Retained state and custody

| Item | Where | Bound |
| --- | --- | --- |
| Immutable admission bit | `State.DebugAttach`, `Config.DebugAttach` (nil restores the recorded bit) | one execution; false↔true on replacement is `attach_admission_conflict` |
| Content-free metadata | `State.Debug`: writer state/revision, terminal facts, input receipts, interaction correlation, retirement | 32 facts, 32 inputs, 16 interactions |
| Volatile tail | in memory, `SetDebugObserver` copy before adapter discard | 8,388,608 encoded bytes, 900 s, 65,536-byte frames |
| Private journal | `<state root>/debug-attach/input.journal`, 0700/0600, same owner, single link, no symlink traversal | 4,194,304 bytes including 65,536 protected bytes and a 512-byte receipt reserve per accepted frame |
| Grants | in memory only, 256-bit opaque secret, 5-minute expiry, delivered only over a caller-owned private pipe/socket | 16 live grants, 16 revocations |

The snapshot never holds a body: an accepted text or answer is journaled as the
whole typed intent and referenced from the snapshot by journal sequence and
digest, keyed by authenticated actor plus client input ID. Terminal facts carry
their original observation time and notification debt; acknowledgement clears
the debt, never the delivery block. A pending native request keeps its
correlation members, options digest and host-clock expiry in the snapshot; the
offered option labels live only in its private journal frame and bounded host
memory, and an answer must name one of them.

Ordinary status (`Host.Snapshot`, the server status response) is an
allowlisted diagnostic projection: writer state and, per terminal fact, input
and interaction, only its outcome/kind/stage/state, ack/permission/debt flags
and relative age in seconds. Result IDs and revisions, native thread/turn
correlation, absolute timestamps, input IDs, actors, journal sequences,
payload-derived digests, interaction handles, native request IDs and options
digests never leave the private state file, the journal and the grant-bound
control view (`debugControlView`), which a current view grant uses for exact
disposition and answer correlation and which returns only the caller's own
receipts.

A grant retains the complete binding it was issued under. A grant issued while
the host knew no native thread is pinned once, by the reaper, to the first
thread that appears; any different or lost thread afterwards reaps it. Every
close and sync on the journal and snapshot commit paths is checked; a failed
close or sync during retirement leaves retirement incomplete for retry, and an
interrupted snapshot commit leaves its pending marker so replacement holds.

## Admission order per private operation

Identity and grant validity first (public `attach_unauthorized`,
`attach_grant_revoked`, `attach_in_use` at the attachment boundary), then the
operation rights (`input_right_required`), then exact historical retry or
conflict, stop, lease, stale observation, safety fence (including a pending
protocol outbox frame, shared with explicit resume), kind eligibility,
capacity/storage and finally the immutable reservation. An answer additionally
requires the host to positively know its current native thread and the
retained request to belong to it; a request from a superseded thread, or any
request while the thread is unknown, is `interaction_stale`. A grant whose
private delivery fails on any path, including a missing destination, is
revoked on the spot (journaled in protected capacity) and can never attach
later; an unknown token is a plain refusal that revokes nothing. A refused attempt
creates no effect, record or journal frame; only a bounded content-free
rejection counter is kept for the diagnostic summary.

Rights per operation follow contract C07: `view` reads, acknowledges and
detaches; `view+writer_control` claims, renews, yields, dispositions and resumes
(`resume_automation`, `yield_and_resume`, `detach_with_resume`);
`follow_up` additionally needs `text`; `answer_interaction` needs
`native_response` and no text right; stop needs `view+stop` and no lease.
Grant issuance and revocation need the registered owner, or the registered
manager for its own grant.

## Effects, receipts and recovery

The host owns the gate; the effect itself is behind an internal effector seam.
No production effector is installed, so `follow_up` reports
`input_operation_unsupported` and `answer_interaction` reports
`interaction_unsupported` until unit D qualifies a native adapter. The B
fixtures install an inert effector that only counts and retains frames.

An accepted input is journaled, persisted as `issued`, released from the state
lock for the effect call and then settled as `write_known` or `effect_unknown`
with a receipt frame that consumes its reservation. An ambiguous result is
returned as `input_effect_unknown` while its receipt is retained. An exact
retry by the same actor, ID and whole intent returns the retained receipt
without a new sequence or effect; a changed intent under that key is
`input_conflict`; another registered actor using the same ID is a distinct key.
Grants retain the complete binding (execution, runner incarnation, host epoch,
native thread) and the manager/owner registration they were issued under and
are revalidated on every use; a changed registration, runner or thread reaps
them and holds a writer they owned. Replacement reconstructs the
journal, turns an `issued` marker without receipt into `effect_unknown`,
retains a journal frame without snapshot reference as an unknown effect and
marks an unframed tail torn; a torn or unknown state holds new effects and
automatic delivery until retirement, while stop, revocation and controls stay
reachable. Retirement requires stop, explicit abandonment of unknown effects,
and a successful logical purge with parent sync; any failure leaves it
incomplete for retry.

## Sinks

The only diagnostic export is a content-free summary (fixed classes, counters,
relative ages, verdicts). Private results use the closed Appendix B envelope
and never enter public status. Fixtures scan the snapshot, status response,
summary, rendered results, notifier intents, ordinary state files, workspace
and the real inert child's argv, environment and inherited descriptors for
synthetic grant, body, option, question and path canaries, paired with a
detector-control child that must see a deliberately passed sentinel.

## Verify

```sh
CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go vet -p 1 ./internal/sessionhost ./internal/codexadapter ./internal/runner ./manager
CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 -timeout=5m ./internal/sessionhost ./internal/codexadapter ./internal/runner ./manager
```

The debug-attach fixtures are the `Test*Skeleton`, `TestAttach*`,
`TestGrant*`, `TestJournal*`, `TestTerminal*`, `TestClaimVsNotice*`,
`TestRetry*`, `TestRevoke*`, `TestNativeAnswer*`, `TestText*`,
`TestRetirement*`, `TestExportAndSink*`, `TestDisabledExecution*`,
`TestDetachLeaves*`, `TestLeaseYield*`, `TestOrdinaryStatus*` and
`TestReceiptKey*` tests in `internal/sessionhost`.
