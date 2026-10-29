# Flight Logbook

> Institutional memory. Concise, factual, high-signal.
> Newest entries first. One block per insight.

## 2026-10-29

### 1825 — Darwin child reap and signal serialization
- FINDING: Go 1.25.5 `os/wait_unimp.go` returns false for Darwin's waitability precheck; retaining an `os.Process` alone does not close its concurrent wait/signal PID-reuse window.
- FIX: `internal/runner/runner.go` serializes nonblocking `wait4` and signals; the child PID remains allocated until that reaper consumes exit, then its handle is retired. No stored-PID adoption.
- SCOPE: Synthetic M1 children only; launcher process-incarnation proof and process-tree containment remain separate qualification.
