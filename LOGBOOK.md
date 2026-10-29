# Flight Logbook

> Institutional memory. Concise, factual, high-signal.
> Newest entries first. One block per insight.

## 2026-10-29

### 2038 — Native command receipts and turns need separate identities
- FINDING: A queued protocol command can be accepted before a native turn exists. Storing that command receipt as the turn ID makes a later valid completion impossible to correlate.
- FIX: The shared transition and durable notice record distinguish native receipt and turn IDs. Synthetic Codex, Claude stream-json and Muse MSP contract fixtures pair valid completion with wrong receipt/input/session rejection and verify rejected facts change neither state nor disk.
- LIMIT: Claude/Muse examples are schematic contract vectors, not native qualification. Codex 0.164.0 has separate disposable two-turn native evidence.

### 2035 — Recovery must observe uncertain writes without resending them
- FINDING: A host may die after journal commit but before recording a pipe-write reply. Reissuing an absent outbox action during recovery creates an effect that the prior execution never proved.
- FIX: Authenticated runner write receipts are read-only and payload-digest bound. Replacement reconciles only recorded writes; missing/unknown input remains fenced and conflicting receipts close processing. Transport acknowledgement, native acceptance and successful completion remain separate.
- VALIDATION: Synthetic child recovery covers written/missing/conflicting effects. Intentional resend and acceptance-as-completion plants each trigger the specific negative assertion. Raw incoming output stays out of the durable checkpoint.

### 1855 — Accepted connections must share the shutdown admission boundary
- FINDING: Closing a listener does not dispose of a connection already returned by accept but not yet registered. A late controller could escape the shutdown connection sweep and retain an unbounded idle read.
- FIX: Registration and shutdown synchronize on the same lock; closed runners reject registration and claims. A deterministic accepted-connection interleaving is paired with live controller admission and bounded shutdown.
- VALIDATION LIMIT: Fixture same-user sockets; this does not establish cross-user isolation or a real harness lifecycle.

### 1825 — Darwin child reap and signal serialization
- FINDING: Go 1.25.5 `os/wait_unimp.go` returns false for Darwin's waitability precheck; retaining an `os.Process` alone does not close its concurrent wait/signal PID-reuse window.
- FIX: `internal/runner/runner.go` serializes nonblocking `wait4` and signals; the child PID remains allocated until that reaper consumes exit, then its handle is retired. No stored-PID adoption.
- SCOPE: Synthetic M1 children only; launcher process-incarnation proof and process-tree containment remain separate qualification.
