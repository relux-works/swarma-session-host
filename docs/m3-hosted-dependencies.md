# M3: macOS hosted-launch dependencies

Revision 2, 2026-10-29; required product contract. Goal: one admitted Curator execution completes a turn, survives host replacement with the same runner/native session, completes another turn, then receives an owned stop. The synthetic implementation does not implement this live route.

These are required semantics, not extensions to an existing closed wire schema. The launcher publishes the accepted version, field spellings, capability discovery and compatibility rules before integration. Baseline: launcher FRONT-DOOR §§5.3/5.7 and README “Hosted home leases” at `8ab9cb30735a159589649d32c6544f3b99ebec3a`.

## 1. Admitted plan and session record

| Required non-secret fields | Writer and publication point |
| --- | --- |
| Schema/version; launcher/manager registration incarnation; operation ID; stable session handle; execution generation; monotonic record revision; explicit lifecycle (`prepared`, `started`, `exited`, `unknown`) | Launcher assigns the operation/registration binding before submission; manager publishes its durable record before acknowledging each transition. Existing identifiers may supply these semantics; lifecycle is a separate new member of the next record version. |
| Admitted-plan reference/digest; runtime; profile name and lock/fragment digests; canonical workspace/home identities; transport; effective permission posture; opaque vendor/model/effort; qualified native release and executable identity | Launcher resolves/admission-checks before start; manager verifies the accepted non-secret projection and binds it to the execution record. No second model registry. |
| Runner instance and endpoint identity; role-labelled process proofs for the runner, Curator wrapper and actual harness; native session ID when the protocol establishes it; owned tmux server socket path/server PID, session ID `$N` and pane ID `%N` | Runner reports its own process and direct child after creation; the launcher/provider observer identifies the actual harness. Manager publishes `started` only after the complete required proof is verified. Native identity may be pending until handshake; pending is never ready. |

The lifecycle is independent of the six-valued §5.4 `state` and §5.7 operation-journal phases; it must never be inferred from `state`. `exited` is neither `down` nor lease-release evidence.

The host reads the record through the admitted manager observation channel, validates closed versions and exact operation/registration/generation/plan bindings, then compares them with its journal, authenticated runner and current kernel observations. Record revision cannot move backwards; changed identity within a generation is a conflict. Native session ID must agree with the adapter. Before input eligibility, verify the intended profile/permissions, actual harness release, live tmux ownership, transport handshake and successful lease bind. For this slice, `pane_pid` must equal the proven runner PID, whose direct child is the proven Curator wrapper; an incompatible topology refuses. Verify server PID incarnation and query that exact server socket for session/pane membership. A process or pane existing alone proves none of these.

Missing, stale, malformed, unsupported or conflicting evidence refuses admission/reconnect before input or another spawn. Loss after start closes the delivery fence and retains the handle and uncertain operation; it never selects native fallback or creates a replacement generation.

## 2. Process incarnation

Each proof contains machine/boot identity, PID, effective UID, kernel process-birth value at full kernel precision, process role, executable identity, execution generation and observation result/time. PPID/PGID are diagnostic only. The actual harness proof cannot be replaced with the launcher/runner PID, a session creation timestamp or a public random runner ID. Exec can retain PID/birth, so executable identity and harness handoff must also be checked.

The process owner captures proof at creation/exec handoff; the admitted macOS observer independently re-reads the same tuple before admission, reconnect and any PID-based decision. Both readers use `proc_pidinfo(PROC_PIDTBSDINFO)` for `pbi_start_tvsec`, `pbi_start_tvusec` and effective UID, and `kern.bootsessionuuid` for boot identity; unavailable evidence refuses. Executable identity uses the running image's code-signing cdhash, not a path. Curator's holder takes only the actual harness `{pid, started_at}`: UTC RFC 3339 with exactly six fractional digits on macOS, equal to the kernel seconds/microseconds. The full boot/UID/role/executable/generation tuple stays in the session record or observation v2 and is verified by the host and Curator observer. Extending the holder itself requires a separate curator-spec decision; M3 assumes none. An unobservable, exited, reused or mismatched process yields explicit unavailable/conflict, never “running” or confirmed absence. Observation time alone cannot establish identity.

The runner keeps exclusive stop authority over its directly owned unreaped child and serializes signal/reap. The host sends only execution-bound runner commands, never signals a PID recovered from a record. The observer does not solve the check/signal race for arbitrary processes. Harness descendants are observed, not claimed confined or automatically killed by M3.

## 3. Home lease

| Required evidence | Writer, timing and host check |
| --- | --- |
| Lease API/schema version, stable acquire request ID, canonical home/environment/profile and admitted lock digest, holder identity | Launcher durably journals the admitted route/request before acquire. Curator is lease authority; host verifies the returned tuple against the admitted plan. |
| Lease ID, CAS revision, authoritative disposition (`prepared`, `bound`, `released`, `broken`), manager registration, session handle/generation and actual harness incarnation | Launcher journals acquire before possible submission and binds through the public API immediately after authoritative start. Curator independently validates the process/session evidence and publishes the binding. Host requires an authoritative matching bound observation before automatic input. |

The holder names the stable execution, not the replaceable host/controller PID. Host replacement revalidates the same binding; it does not acquire a successor lease. Lost acquire replies replay the identical request; possible submission recovers by status/cancel, never launch replay. Lost bind replies reconcile/retry only the identical binding with current authoritative revision. Conflict, unsupported route/API/observer, incomplete inventory or unknown disposition closes admission and retains protection. Released/broken acquire replay makes no manager calls.

The launcher must provide both the authoritative session-record projection and an independently accepted Curator observer/bind path. Record fields alone cannot unblock current bind refusal. Host exit, detach, timeout and root-child exit never release the home. M3 invokes no release/break and never edits Curator storage; later release needs separate authoritative absence and authority contracts.

## 4. Synthetic scope and exit evidence

The synthetic unit adds a versioned host admission seam, injected process/lease observers and metadata-only persistence, plus an owned synthetic runner/child fixture. Pair successful bind/reconnect with wrong generation/registration/UID/boot/birth/executable/plan/home, revision rollback, unsupported versions, missing native identity, PID reuse, lease conflict and observer outage. Assert zero unauthorized writes/spawns/signals, no lease release, retained uncertainty and no replay; repeat host SIGKILL/reconnect with the same execution. Use bounded control plants to demonstrate that identity and lease gates detect their named violations.

Synthetic providers never run Curator, acquire real leases or claim launcher compatibility, kernel PID-reuse qualification, cross-UID isolation, live-member liveness or descendant confinement. Real M3 acceptance waits on three external deliveries: (1) the versioned record/observation v2 from curator-agent-launcher, published by the launcher project; (2) Curator draft PR #194 landing and release through its release flow; (3) a production Curator observer accepting the full evidence for HL22 bind, provided by Curator. The pinned launcher requires all three; synthetic work waits on none. Acceptance also requires an explicitly admitted disposable macOS run through Curator with its own validated profile, yolo, owned tmux and final liveness evidence. Existing adapter boundary/capabilities remain unchanged.
