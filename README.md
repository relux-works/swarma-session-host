# swarma-session-host

The session host provides provider runtimes for Codex, Claude Code and Muse, plus authenticated local control transport, session records, attachment lifecycle and client transport. It is a standalone Go module. Board packages are not runtime dependencies.

## Implementation status

The source assembly includes `sessionmanager`, `sessiongoal`, `sessionbudget`, `sessioncontext`, `musepty`, `sessionplugin`, `sessionclient`, `musebuilds` and `hostdefaults`. Provider launch payloads use the shared typed contract; foreign and ambiguous payloads are refused before provider I/O. Original tests are preserved alongside additional boundary tests; their presence alone does not claim complete test parity.

`cmd/tb-sessiond` now composes the actual host, durable goal/accounting/context services, provider factories, captured owner renderer and authenticated control server. Its default has no external exact-session authority registrar or full legacy revision proof, so those operations remain fail-closed; this is not complete standalone acceptance. `hostdefaults.FileConfig` supplies explicit file-backed configuration. `sessionmanager.NewSessionNoticeStore` supplies the original structured notice queue, and the real host delivery pump acknowledges successfully delivered bursts. `NewLegacyRecordCodec` preserves original record images and provider subrecords; active native accounting restoration still requires its real runtime supplier. `hostdefaults.NewMaterializationHostCodec` connects that decoder to the context catalog while retaining cancellation and unsupported-state refusals.

`NewProtocol8Handler` supplies physical session routes for the authenticated `NewControlServer`. Captured owner runs use `sessionrun` through the core host. The actual core takeover and doctor paths are composed. Status reads the real context catalog and quarantine inventory; the legacy goal descriptor remains absent until an enforcing validator is bound. Legacy launch conversion and complete active restoration still require their exact source closure. `sessiongoal` supplies the durable goal journal and binding factory; `sessioncontext` supplies the context catalog, recovery, retention and materialization engine. Their production authority, observation, native accounting, provider materialization and fork-validation suppliers remain required. See [the exact composition boundaries](UNRESOLVED_QUESTIONS.md). Building these components is not evidence that a complete daemon can serve the existing capability set.

Goal authority belongs to the launcher of record and an authorized operator. Assignment goals and a session's working goals are distinct. Durable source envelopes, holder revisions and old-holder refusal are required; a provider banner or process exit cannot accept a goal. Identity binding stays on the raising side until a named replacement exists. These requirements do not introduce a board callback or board runtime dependency.

## Standalone command

Build the retained executable entry point with `go build -o .temp/tb-sessiond ./cmd/tb-sessiond`.
Run `.temp/tb-sessiond --state-root /absolute/private/state --namespace selected --config /absolute/host-config.json`.
Omit `--config` only to use the optional `config.json` inside the selected state root.
`--socket-path /absolute/private/control.sock` permits a shorter Unix socket location.
`--version` prints build metadata without opening runtime state. The foreground command
uses the existing shutdown signal watch and the core server's recovery, authentication,
self-check and joined shutdown. It does not discover or load a board.

`hostdefaults.OpenStandalone` accepts the genuine external goal authority, retained
legacy source and context supplier ports. It installs the concrete full legacy
validator/proof over that source; the default source remains absent. The command currently has no raising-side
registrar for those sources. Owner strings, request PIDs and a shared OS user never
supply grants. Assignment and working heads remain distinct; native completion does
not accept either. Legacy attach conversion and active native restoration remain
open source boundaries, as listed in `UNRESOLVED_QUESTIONS.md`.

## Tools

Use Go 1.26.8 with `GOTOOLCHAIN=local`, `GOWORK=off` and `GOFLAGS=-mod=readonly`.
The component CI runs these commands on Ubuntu and native macOS; both jobs gate:

```sh
export GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly
# Use a short, owned directory for Unix-socket fixtures on either platform.
SESSIONHOST_TEST_SOCKET_ROOT=$(mktemp -d /tmp/sh1.XXXXXX)
export SESSIONHOST_TEST_SOCKET_ROOT
chmod 700 "$SESSIONHOST_TEST_SOCKET_ROOT"
umask 022
go mod verify
go build -p=1 ./...
go vet -p=1 ./...
go test -p=1 -parallel=1 -count=1 ./...
rmdir "$SESSIONHOST_TEST_SOCKET_ROOT"
```

The suite includes every package in this module, including the goal, context, budget, defaults and command packages, without a test-name filter.

The separate Linux `codex-source-race` workflow preserves the source-replacement
and concurrent-uncertainty reviewer regressions under the race detector. With
the same owned socket fixture directory, it runs independently of the full suite:

```sh
CGO_ENABLED=1 GOMAXPROCS=2 go test -v -race -p=1 -parallel=1 -count=1 -timeout=180s ./sessionmanager -run '^(TestRev5BImageReplacedDuringRead|TestRev5BConcurrentUncertaintyPublication)$'
```

These are bounded fixture checks; they do not waive a failing full suite.
Their output goes to the CI log; retain local logs under `.temp/`.
`umask 022` matches the executable-mode fixtures; the socket setting affects
fixtures only. Provider smokes, opt-in mutation suites and platform-specific tests
retain their own prerequisites and explicit skips. These jobs do not establish
live-provider, complete standalone-host or Curator acceptance.

`hostdefaults.FileConfig` reads an explicit selected file with the existing `session_manager.providers` settings and context policy fields. Only an optional missing default file uses the former 30-second acknowledgement timeout, sleep enabled and 50,000,000-token default; invalid explicit settings refuse. `Load` retains Codex launch defaults, validated optional retention knobs and their original bytes. The standalone constructor binds this actual file source to the context service. `ConfiguredContextRetentionSweep` resolves all nine fields before forming a whole sweep request and propagates source errors before effects. Explicit HTTP sweep requests keep their original independent policy semantics. The public unbound `FileConfig.Snapshot` still refuses configured retention. See [`hostdefaults`](hostdefaults/README.md) for the exact boundary.

`sessionmanager.NewSessionNoticeStore` wraps the original durable inbox without changing its JSON. Pending notices retain the complete structured payload for whole-burst rendering. Receipt IDs name readable queue files, and acknowledgement refuses changed or unissued receipts. The real host delivery pump acknowledges successfully delivered structured bursts. The standalone constructor binds this store; unit queue qualification is not daemon readiness.

`gofmt -l` reports formatting drift. `go mod tidy` updates module manifests from the actual source graph; `go mod verify` checks downloaded module contents. These commands write only the normal Go caches unless an output path is supplied. Put local logs and temporary artifacts under `.temp/`.

The historical extraction tools under `tools/sh1` and their fixture remain documented in [`docs/sh1-gate.md`](docs/sh1-gate.md). They do not establish standalone runtime qualification. Public component CI uses only this module.

Python 3's standard-library witness checks the full command and explicit
workflow/job/step `GOFLAGS` declarations, accepting only single-line literal
`-mod=readonly`. Deeper continuation lines refuse, including across blank or
comment lines.
It does not run component tests or verify arbitrary scripts, `if` guards or
`continue-on-error`:

```sh
python3 -m unittest discover -s tools/sh1/ci -p 'test_component_selection.py' -v
```

`gofmt -l` reports formatting drift. `go mod tidy` updates module manifests from
the actual source graph; `go mod verify` checks downloaded module contents.
Commands write to stdout/stderr and the normal Go caches unless overridden.
Put local logs, list inventories and temporary artifacts under `.temp/`.

The historical extraction CLI under `tools/sh1` and the separate tooling CI are
documented in [`docs/sh1-gate.md`](docs/sh1-gate.md). Public component CI uses
only this module; it has no private checkout or private authentication input.

## Design references

The existing [design specification](spec/session-host.md) and figures include earlier extraction and future platform proposals. They require alignment with the standalone composition and goal authority described above; their former board composition path is not an active runtime dependency.

In the gated SH2+ launch path (R-START/R-SESSION), the owner's admitted start can carry **session lifecycle authorisation** for successors; its member format is unpublished. The dispatcher owns a durable **host admission permit** for one `logical_session_id`. Every physical start has its own `(execution_id, controller_epoch, op_id)` claim, fence and same-effect/root-receipt interlock. The host prepares durable intent and a closed fence, then sends `host_start` in `dispatch-op/1` with the four fixed request values. Only the dispatcher invokes `swarma-user-launcher`. Successors require the owner's lifecycle authorisation, an active owner and fresh authority checks, confirmed absence of the previous claim, the same allocation/generation and unchanged stored plan; replay of `(logical_session_id, op_id)` returns the same successor. A one-start request admits no successor. The host gains no caller chain or spawn/lease grant; the original owner's ancestry remains. Ordinary successors cannot extend allocation lifetime or reset runtime budgets (§3.3).

Controlled restart, park of a wedged execution and emergency stop use `host_stop` in `dispatch-op/1`. Only the configured host principal may stop an execution under its permit, through the journaled E-stop path (`exec.stop`, confirmed absence). It is recorded as host-requested, distinct from owner `cancel`, and grants no start authority. The user manager owns the single account-wide termination scope, latest-execution receipt fence and quiet-period absence proof (F-13). The fresh runner separately checks native-lock absence under the agent account and closed delivery fence before harness initialisation/resume; an unknown result stays parked. Current-epoch registration and readiness precede delivery; ambiguous starts resolve the same effect/receipt, never a second launch. Joint formats and crash ordering remain publication gates (§7).

The dispatcher is a Unix-socket service from its first protected-launch slice (F-5). Once state ownership, compatible schema, journal replay and authentication/indexes are sound, authorised control, observation, reconciliation and already-authorised stop remain reachable while forward admission is closed (F-15). Unknown records retain their claims. Unrelated admission needs complete accounting and proof of confinement; unbounded shared uncertainty, maintenance or restore keeps forward admission closed node-wide. There is no command fallback (§3.3).

The host service has deny-all network egress, including loopback TCP (R-SVC-EGRESS); readiness requires the user manager's verified v1 service-profile row, parent anchor and qualified resolver restriction. Periodic re-verification has a detection window. Runner survival and the affected set on service replacement follow the user manager's §7 **fate table** (R-FATE), with a durable daemon-instance fence against surviving dispatcher writers; deployment defaults do not prove survival. The host MUST never hold its state lock across a synchronous call to another platform service (R-LOCK): durable per-record in-flight markers preserve ownership, and stale results cannot reopen delivery. See spec §§3.1–3.5 and the [host wait/lock graph](diagrams/host-wait-lock.puml).

Spec §3.1 references E-rebind and broker **binding replacement** (R-REBIND), fresh random 128-bit generations and fail-closed ledger restore (R-GEN), and the authenticated or operator-controlled time source and authority-narrowing clock rules (R-TIME). Expiry-driven destruction also requires real boot-inclusive monotonic elapsed time; wedge deadlines never impose standing-session expiry (§3.7). Across isolated accounts a child gets a standalone repository materialized by the dispatcher’s workspace contract with its own `.git`, fresh local configuration, no hooks, no remotes and no credential helpers; linked worktrees remain within one account, including today's board workflow (F-16).

For trusted standing agents, the executor checks plan-provided settings, hooks, MCP configuration and instruction-file digests before every launch, restart and native resume, or supplies them from a root-owned location outside the home (R-STANDING). Drift refuses the launch and is reported; repair is explicit re-materialization and the refusal code is unpublished. Same-machine native resume trusts non-breaking harness semver changes: the same major, and for 0.x the same minor, with the continuity preconditions in spec §3.5. A different major or 0.x minor is refused. A harness resume failure is reported as `resume-failed` to the caller, which decides the next action; there is no automatic fallback or portable seed on this path.

For SH2+, the owner decision of 2026-10-12 gives trusted-class standing sessions (the orchestrator and standing team members) and their executions no preset lifetime or runtime limit. Only an explicit stop, operator action (including emergency stop) or its own exit ends a standing session. Park and controlled restart preserve the session; restart replaces its execution. The dispatcher's 12-hour runtime maximum applies only to non-standing executions. Operator maintenance uses the dispatcher's bounded maintenance stop (R-MAINT, dispatcher §11): close admission, inventory, stop selected executions through `exec.stop` with explicit operator authority, then require confirmed absence and reconciliation before mode/state/binary changes. Its deadline bounds the attempt, never the session lifetime; unknown or refused stop defers maintenance. Standing allocations and identities remain; R-MAINT withdraws the permit, so restart needs fresh owner admission; there is no live supervisor adoption or automatic expiry. Recovery controls remain reachable while forward admission is closed. See spec §3.5 and §3.7; the runtime-policy wire format remains unpublished.

The owner decisions of 2026-10-13 apply to SH2+ (§§3.1–3.5, 3.7). **R-UNTRUSTED/R-STANDING:** an untrusted standing placement is a workplace with its agent loop at the donor, never a host session; class is recorded at registration/admission, never inferred from a name. Workplaces have zero egress, no subscription binding or lease, and no shared services on their host. Every workplace is untrusted-class unless its remote worker is admitted to the trusted circle by an operator act. Membership permits donor-side opt-ins only and preserves all host boundaries, including reviewer and host-loop eligibility. Host loops require trusted class and local agent placement with admitted launch authority; remote placements remain workplaces. Review requires an independent positive role grant and the §3.5 class/placement/producer predicates; membership alone is insufficient. Optional local-only reviewer and requester-exclusion policies stay open and off. Local trusted sessions have provider-only bound-proxy egress. Web search/fetch is default-off for every agent (R-NET-TOOL): by default the donor loop reaches only model-provider endpoints and our host, with provider-side search disabled by CIP-0008 and no donor-side web server. Web tools are explicit per-identity opt-ins for the trusted circle only, never the untrusted class. Tasks carry trusted-side gathered and checked resources, materialized read-only beside the workspace with digests in the plan, and also attached to remote signed task letters; these are data, never instructions. Workers request more through letters/outcomes and never fetch. No `direct` or `external-http-proxy` binds on a platform host. Network-profiles N15 owns one bound listener per account generation, own-port-only rules and attributable caps/logs. R-CEILINGS admits by each host’s qualified OS capability matrix, without excluding macOS by name. R-RANGE-DENY and R-SOCKETS require active range-precedence and socket-reachability evidence.

**R-TRANSFER:** approved child-UID executors materialize and export under the dispatcher’s workspace contract; the host gains no transfer authority. Code leaves only as data after account-wide confirmed absence and any workplace write fence. Deletion requires digest/size-verified durable custody of the result and all blobs outside the home, or a per-act operator waiver. Cleanup export uses bounded original-admission consent and distinct execution evidence, never an R-SESSION permit. Deterministic refusal retains the obligation; same-workspace repair needs current authority and a plan-bounded new round. Unknown export stays pending, and standing discard is unchosen and off. The manifest compares both mode and content identity, including mode-only edits, regular/symlink transitions and prior deletion modes. The board accepts a resource, and its change-request machinery owns deterministic import, exact signed candidate review and integration, executed by the orchestrator. Unaccepted candidate files stay untrusted review data: exact-C diffs/blobs are read-only digest-bound resources outside harness discovery roots. Tree entry in a discovery-capable harness requires a qualified strict-context adapter; acknowledgement and acceptance never admit control surfaces. Rework is a new admitted run on the last candidate with findings in the task snapshot. Worker accounts never push or open merge requests. See the [transfer and verification figure](diagrams/workspace-transfer.puml).

**R-EXEC/R-VERIFY:** the runner starts only the admitted harness under an OS executable allowlist. Protected machine policy selects and pins the exec-profile identity/body digest; the caller cannot choose authority through home configuration. Local light tools require complete-invocation and interpreted-input qualification; unprovable profiles route to off-host verification. Builds, tests and worker-code execution run only on another machine in isolated ephemeral runners without secrets, with controlled dependency egress; results are data in the board’s testing state. A tester orchestrates and reads them. R-WATCH v0 requests a journaled dispatcher stop and retains the affected session’s closed delivery/admission hold, with durable reason and status-only observer reads; no phone alert or per-account egress freeze is promised. The v1 freeze lifecycle remains open. Containment never waits for notification. **R-ALLSTOP** is one manual operator act delivered directly to each service. Range denial, broker freeze and host closure issue promptly and independently; receiver launch/create/activation fencing precedes absence scans, and process stops never wait for containment replies. The operator helper path can progress while the dispatcher is unavailable. The host closes every session’s delivery and snapshots bound boards. Already-delivered tokens keep vendor lifetime. Fences remain until confirmed absence; all durable parts, including dispatcher reconciliation, are required before whole-act success or explicit recovery. Recovery names the stopped set and restart scope and preserves owner authority; partial/unknown results keep admission closed. See the [node-wide stop figure](diagrams/node-emergency-stop.puml). Formats, codes, caps, runner contracts and the per-host soft-ceiling decision remain open in spec §7.13–18.

For gated SH2+, the unchanged `(principal, id)` claim stores/compares complete canonical signed intent via `envelope_digest` through host enqueue, deduplication and receipt lookup: exact-byte retry returns the original receipt; changed intent conflicts and requires a new ID. Host-owned output, input and evidence have per-session/principal byte/count and aggregate budgets, charged before durable acceptance. Retained debt survives replacement/retirement; bounded buffers and the dispatcher-output handoff preserve a protected control/evidence reserve. Budget pressure uses bounded refusal/backpressure or existing journaled park/stop, while incomplete evidence stays closed/unknown. Values, retention, schema and qualification are open in spec §7.19–22; no implementation is claimed.

Design input: [hosted-launch contract r6 harvest](docs/inputs/hosted-launch-contract-r6.md), from agent-session-host PR #10 at its recorded head. Its task-board receiver is superseded by the swarma dispatcher path; the retained seam, admission-record and imported-Claude notes are input, not an activated contract.

Related modules: [swarma-dispatcher](https://github.com/relux-works/swarma-dispatcher), [swarma-user-manager](https://github.com/relux-works/swarma-user-manager), [swarma-credential-broker](https://github.com/relux-works/swarma-credential-broker), [curator-agent-launcher](https://github.com/relux-works/curator-agent-launcher) (`curator-run`), [agent-session-bridge](https://github.com/relux-works/agent-session-bridge).

The [figure index](diagrams/README.md) maps audit rows M-SH1–M-SH4 and the R-LOCK view M-SH5 to their
PlantUML sources and defining spec sections:

- **M-SH1:** [process/container ownership](diagrams/containers.puml) and
  [logical components](diagrams/components.puml), with current/SH1 and gated
  SH2+ boundaries.
- **M-SH2:** [controller recovery and restart](diagrams/session-recovery.puml),
  including confirmed-absence and unknown-outcome branches.
- **M-SH3:** [session, execution, goal and wake states](diagrams/session-goal-wake-states.puml)
  and [approval/receipt states](diagrams/approval-receipt-states.puml).
- **M-SH4:** [storage ownership](diagrams/storage-layout.puml) and
  [SH1 gate deployment](diagrams/sh1-gate-deployment.puml), separating the
  current hosted prototype from the release gate.

- **M-SH5:** [host wait/lock graph](diagrams/host-wait-lock.puml), showing local lock transitions and unlocked service waits.
- **R-TRANSFER/R-VERIFY:** [workspace transfer](diagrams/workspace-transfer.puml), showing export, exact candidate review and admitted rework.
- **R-ALLSTOP:** [node-wide emergency stop](diagrams/node-emergency-stop.puml), showing manual act, service journals and explicit recovery.

The figures mark unpublished schemas explicitly and establish no runtime
capability. Each links back to its defining specification sections.

## Muse build management (`musebuilds/`)

The `musebuilds` package discovers installed Muse builds, selects the newest
one, and owns host admission, freeze publication, first probe and durable
last-good state. Its `minilock` and `probeprocess` subpackages provide the
build lock and the owned probe-process launcher. It supports Linux and macOS;
other platforms refuse before I/O. Build and test it with:

```bash
go test ./musebuilds/...
```

The narrowing-mutant suite in this package is explicit opt-in and is not
run by the default test command. To run it manually, with the Go toolchain
for this module on `PATH`, execute:

```bash
TASK_BOARD_MUSE_RUN_MUTANTS=1 go test -tags mutants -run '^TestMuseBuildsNarrowingMutants$' ./musebuilds/ -count=1
```

## Provenance

Relocated components retain their original Apache-2.0 attribution and tests. The NOTICE preserves the original attribution. Source revisions and validation evidence are recorded with each integration candidate.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE) for licensing and attribution.


The same built `tb-sessiond` implements the original internal
`session context-gateway-shim` argv used by provider MCP configuration. The
command selects its own concrete executable and supplies the exact shim argv to
`OpenStandalone`; the runtime library requires explicit command selection.
Gateway endpoint/authentication comes from the existing provider-specific
`TASK_BOARD_BUILDER_GATEWAY_SOCKET` and `TASK_BOARD_BUILDER_GATEWAY_TOKEN`
environment, not daemon credentials. MCP responses go to stdout. The shim
creates no host or board and retains ordinary child-process signal behavior.

`OpenStandalone` binds the same concrete builder gateway runtime to the context
service when a shim is selected. Explicit `Standalone.Close` joins its owned
surfaces and preserves missing-native-proof errors. Provider Start/Bind and the
core server's pre-observation-close ordering still require their real owning
source; default construction is not a claim that native completeness or a
canonical signed publication issuer exists. Active PTY revision delivery now
uses the actual owner runtime with the same budget/service supplier; native
accounting restore and deferred Codex owner continuation retain their guards.
