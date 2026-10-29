# Machine-local context runtime

`sessioncontext` implements the host-owned durable materialization catalog,
exclusive run leases, restart reconciliation, publication orchestration,
provider presence/recovery policy, retention/reclamation, and measured economics
and completeness stores. It imports the public `sessionmanager` contracts and
standard library. It has no task-board dependency, client, callback, canonical
registry constructor or business ledger.

`Open(ctx, Options)` requires explicit private layout paths. Authority, current
host references, local policy/config, actual provider factories, validation and
provider preparation, legacy host conversion,
and builder gateway/enforcement are distinct inputs. A missing input refuses
its operation; it is never a claim that the capability is supported. Authority
implementations must bind the request context to legitimate external
launcher/spawn/trust facts. A requested owner or target string is not identity
proof and cannot grant a permit.

## Runtime methods and ownership

- `CreateMaterialization`, `AcquireMaterializationLease`,
  `ReattachMaterialization`, `CompleteMaterialization`, `FailMaterialization`
  and `RerouteMaterialization` implement the original durable transitions.
  These lower-level methods accept trusted host observations; the HTTP boundary
  must use the run-facing methods below, which derive the private handle/digest.
- `PrepareRunMaterialization` checks exact active run/context/target and child
  lineage against authoritative facts before preparation. Cold preparation records
  the truthful empty prelaunch handle. Forks call the separate real
  `ContextForkValidationSource` for current policy, preflight and source-derived
  boundary; the actual decision and rendered banner reach the provider plan.
  The default Codex path prepares the original typed native fork plan and
  requires a manager-observed completed base turn. Its native Launch remains
  provider-owned. Claude preparation invokes the registered factory's actual provider-owned
  `PrepareContextFork` inspector/normalizer. Missing capability or a foreign/
  incomplete returned plan refuses before child catalog writes.
- `RunCodexContextAssay` and `RunClaudeContextAssay` invoke the registered real
  factory's existing optional `RunContextAssay` method after mirror/current
  source/security checks. The operation mutex is released for provider I/O.
  Claude's retention anchor comes from authority, not the caller. Provider
  payload-loss/incompatibility errors automatically route through recovery.
  Missing optional capability is a typed refusal, never a successful assay.
- `MaterializedProviderHost` rechecks the exact lease, active source run,
  canonical authorization, persisted consumer role and local policy immediately
  before provider I/O. `RecordProviderHost` stores the actual observed typed
  snapshot after the core's sole record writer sees the launch/settlement.
- `CompleteRunMaterialization` derives the live child transcript and scans its
  actual bytes. Publication presents its local path, allowlist and existing
  builder claim to the authority. It does not mint classification or enforcement.
  Publication failure retains the original failed-run/retirement transition.
- `Reconcile` keeps missing, unknown or active run custody. Conflicting evidence
  is quarantined with an unresolved writer identity, which is not collectable.
  Raw corrupt/unsupported catalog entries retain their original bytes and reserve
  their context identity in quarantine. Raw quarantined identities also hold
  artifact references and retention leases; unreadable quarantine metadata is
  unknown visibility and refuses a destructive sweep.
- `ReclaimContextPayloads`, `SweepContextRetention`,
  `MaterializationGCCandidates` and `CollectMaterializations` retain live or
  unknown references. Provider bytes are released before their catalog name.
  Codex adopted/symlinked/unowned files are refused; Claude removal consumes only
  the provider's original named-artifact manifest and records its deletion audit.
- `ProviderPolicy` implements the actual `sessionmanager.CodexContextPolicy`.
  `ClaudePolicyError` translates the actual Claude presence/resume observations.
  The relocated structural scanner consumes the whole transcript and preserves
  authoritative compaction precedence. Unknown presence is not evidence of loss.
- `ContextEconomics`, `ContextForkTelemetry`, `RecordForkTelemetry` and
  `ContextCompleteness` operate on real local observations. Unmeasured spend and
  unavailable instrumentation remain explicit. Completion without an installed
  provider telemetry collector records an unavailable row, never a measured zero.
- `FinalizeBuilderGateway` preserves the original `CtxID`, `Served` and complete
  trace attestation reply. It forwards the actual gateway supplier's result and
  refuses a foreign context/run/provider receipt. No native gateway is implemented
  by this package.

## Core integration boundary

The core remains the sole session-record writer and provider controller. Its
ordinary `SessionHost.Attach`, staged `PrepareAttach`/`CommitAttach`, and
`Recover` must call this service around the actual Launch/Restore path. A
materialization ID is currently refused by both attachment paths in the core;
this package does not claim that those paths are wired.
`NewHTTPHandler(service) (http.Handler, error)` implements the nine context
routes below over the actual Service. It opens no listener. The core must mount
it behind its existing authenticated socket/bearer and host-ordering boundary;
a handler test does not qualify that outer composition.
Core code must preserve its existing controller reservation, cancellation,
rollback, input/goal ordering and persistence fence. It must supply current
reference visibility including recovering or provisional readers. It must
never call this service while holding a core mutex that its reference supplier
needs. Service callbacks run under its own operation mutex, so injected suppliers
must not call back into this service.

The corresponding original protocol8 operations are:

| Original route | Service method / response |
|---|---|
| `POST /v1/contexts/reclaim` | `ReclaimContextPayloads` / `ContextReclamationReport` |
| `POST /v1/contexts/sweep` | `SweepContextRetention` / `ContextRetentionSweepReport` |
| `POST /v1/contexts/economics` | `ContextEconomics` / `ContextEconomicsReport` |
| `POST /v1/contexts/completeness` | `ContextCompleteness` / `ContextCompletenessFeedback` |
| `GET /v1/contexts/materializations` | `ListMaterializations` / safe views |
| `POST /v1/contexts/materializations/prepare` | `PrepareRunMaterialization` |
| `POST /v1/contexts/materializations/complete` | `CompleteRunMaterialization` |
| `POST /v1/contexts/materializations/fail` | `FailRunMaterialization` |
| `POST /v1/contexts/builder-gateway/finalize` | `FinalizeBuilderGateway` / `BuilderGatewayFinalization` |

The source-split brief called the retention route
`/v1/contexts/retention/sweep`; original `control.go` uses `/v1/contexts/sweep`.
This package adds no HTTP alias. The core route owner must preserve the original
request/response/refusal envelopes during integration. Bodies use the original
canonical snake_case DTOs and a 1 MiB bound, reject unknown fields, and reject
trailing values or limit overflow. The explicit EOF/overflow check strengthens
original `LimitReader` decoding rather than claiming identical permissiveness.
Runtime-only confinement is excluded from JSON and retained only when cloning
verified source facts. Gateway/confinement suppliers receive the actual request
context.

## Explicit supplier bounds

The real `ClaudeHostFactory.PrepareContextFork(ctx, source, input)` export reads
through the original `claudeBaseTranscriptRef`, `claudeInspectBase` and
`normalizeClaudeSupplemental`. It replaces caller Base* claims with actual
transcript identity/digest/count/format and preserves the admitted source
boundary, ready time, mode, target and ordered supplemental inputs. Preparation
performs no provider launch, transcript copy or registry write. The service's
default Claude path calls this existing factory capability, while native Launch
still owns the actual copy/fork and its verification.

The shared record now carries the actual validation evidence/decision/banner
vocabulary and original canonical wire names. Those data declarations do not
implement a production decision supplier. `Options.Validation` must be the
actual current-policy/evidence/coverage owner. A consumer security permit cannot
substitute for `PreflightContextFork` or `BuildContextBoundaryBanner`.

Production ContextAuthority/trust binding, current reference visibility, actual
provider factories, builder
gateway/confinement/enforcement and provider consumer observation collectors
remain distinct owner-supplied inputs. Native provider isolation, attestation,
fork/RPC execution and availability are not inferred from a typed port test.

`MaterializationHostCodec` consumes the actual shared `LegacyRecordCodec`
`DecodeProviderHost`/`EncodeProviderHost` methods. The real codec is installed
by default; an explicit override must implement that same subrecord contract.
The durable store retains unchanged original bytes and unknown private/catalog
members when changing its own lifecycle fields, including clearing old leases.
Unsupported active private goal/owner state remains quarantined intact with
reserved custody. Modern typed snapshots and both public prelaunch fork plans
reopen durably; unsupported old opaque fork-plan formats are not silently
reclassified as supported historical native snapshots.

The library does not transport payloads or implement a remote authority, native
provider availability, a default daemon, hosted checks, or end-to-end admission.
The presence policies retain the source's observed provider retention behavior;
they provide warnings, not a vendor durability guarantee. Filesystem reclamation
retains the source's current-path checks, not a proof against a concurrent
same-user adversarial rename. Service operation serialization is per instance;
the owning core must retain its existing singleton/namespace custody.

## Tools and validation

Use the repository's admitted Go SDK and matching gofmt:

```sh
gofmt -w sessioncontext
go build -p=1 ./sessioncontext
go vet -p=1 ./sessioncontext
go test -p=1 -parallel=1 -count=1 -run '^TestContext' ./sessioncontext
```

On shared machines, serialize resource-intensive validation through the
operator-configured resource scheduler with explicit resource and deadline
checks. Test fixtures use `testing.T.TempDir`; no native provider or daemon is
needed. Task-scoped raw commands, exits, identities and bounded control evidence
belong outside the worktree under `.temp/<task>/evidence`, not in product data.
Pure relocated scanner, economics and completeness tests retain their original
names; original artifact and applicable catalog lifecycle regressions remain
executable. Canonical board-store and core/daemon integration tests remain with
their original owners and are not qualified by this package's tests.

## Original automatic-assay fixture custody

The original tests `TestCodexAssayResumeFailureAcrossBuildsIsRecoveredInTheRegistry`
and `TestClaudeAssayProviderDeletedBaseIsRecoveredInTheRegistry` execute from
`sessionmanager/context_service_assay_test.go` (external test package) against the
actual `sessioncontext.Service`. `context_assay_export_test.go` exposes only the
original safe in-process Codex source and original Claude absence fixtures to
that test build. It is excluded from production GoFiles; no production injection
API was added. The Codex fixture performs actual loopback RPC before automatic
recovery. Claude's missing-base path refuses before its fixture executable runs.
These qualify original receiving behavior, not a live model, native successful
assay/isolation attestation, production authority or complete host composition.

```sh
go test -p=1 -parallel=1 -count=1 -run '^(TestCodexAssayResumeFailureAcrossBuildsIsRecoveredInTheRegistry|TestClaudeAssayProviderDeletedBaseIsRecoveredInTheRegistry)$' ./sessionmanager
```

Use the admitted heavy-slot and pinned SDK/environment on the shared host.

### Retention configuration application

`Options.RetentionConfig` accepts a context-owned `RetentionConfig` source.
Its `RetentionSnapshot(ctx)` returns `RetentionConfigSnapshot{Known: true,
Settings: ...}` only after the configuration owner successfully loads and
validates the original nine optional knobs. Seven durations and two counts map
directly to `sessionmanager.ContextRetentionSettings`; zero means verified
unset. The configuration parser must reject unknown keys, malformed values and
explicit nonpositive values before producing this snapshot. An unreadable,
failed or unknown source must return an error or `Known: false`, even if some
values were available. No parser or authority store is copied into this package.

Call `Service.ContextRetentionSettings(ctx)` and propagate its error before
constructing `ContextRetentionSweepRequest{Settings: settings, ...}`. This is the
original command-side sequence from `context_retention_policy.go` and
`session_context_retention.go`: resolve configured fields, fill unset fields with
runtime defaults, pass the whole policy to the sweep. Omitting the optional
source explicitly chooses the shipped defaults; an installed unavailable source
refuses. The sweep retains its existing explicit-request normalization: partial
requests fill nonpositive fields with shipped defaults, without a field merge
with project configuration. This port does not wire the hostdefaults parser or
HTTP command composition; those consumers must perform this sequence.

Narrow changed-port checks (SDK Go 1.26.8, run via the host heavy-slot):

```sh
go test -p 1 -parallel 1 -count 1 ./sessioncontext -run '^TestContextRetention(Config|ResolvedSettings)'
go build -p 1 ./sessioncontext
go vet -p 1 ./sessioncontext
```

Task-scoped command logs and source identities are retained outside the worktree
under a local `.temp/` evidence directory.

### Builder gateway runtime (original source extraction)

The host-owned `sessionmanager.BuilderGatewayRuntime` now owns the actual
allowlisted repository gateway, authenticated private Unix service, MCP stdio
shim, append-only access trace and terminal trace verification. It contains no
board dependency, canonical registry or signing key.

The executable/core owners compose it in this order:

1. `NewBuilderGatewayRuntime(BuilderGatewayRuntimeOptions{Layout, ShimCommand,
   ShimArgs, Now})` with explicit actual host-owned paths and an installed real
   shim command. That command calls `RunBuilderGatewayShim`; this library never
   launches a provider or installs a command.
2. Before provider launch, `StartBuilderGateway(ctx, record, projectDir,
   allowlist)` returns the actual listening `BuilderGatewayEndpoint` to put in
   the typed provider launch plan. CTX/run/provider come from legitimate source
   facts; the gateway does not establish their authority.
3. After successful launch, `BindBuilderGatewayObserver(ctxID, runner)` requires
   the original `BuilderToolObservationReporter`. Its independent native call,
   foreign-tool, unknown-shape and disconnect verdict is required; shim counts
   alone cannot supply it. The Codex runner reads its actual initialized native
   thread/turn source, rejecting foreign thread identity; Claude reads only the
   actual transcript suffix captured before launch. Missing confinement, native
   source, terminal state or known schema continues refusing completeness.
4. Inject the same runtime as `sessioncontext.Options.BuilderGateways`. Existing
   Service finalization and completion close its actual socket, attest the trace,
   then load/verify the terminal chain and CTX/run/provider binding. Cleanup closes
   an owned surface even on caller cancellation or later authority-read failure;
   those operations return no usable finalization. Failed close/reconciliation
   remains a refusal on retry within that runtime owner.
5. The core lifecycle owner injects the **same** runtime into its existing
   `HostOptions.BuilderGateways` close port and this service. Its joined close
   happens before native observation teardown. Do not add a second runtime or
   pre-close it from executable cleanup and lose the original result.

`FinalizeAndRequireBuilderCoverageEvidence` retains the original **non-starting**
connection ordering. A connector may use `ErrBuilderGatewayNotRunning` only for
positively observed absence; unknown transport errors refuse even if a complete
trace exists. `RequireBuilderCoverageEvidence` checks closed-chain integrity,
not signature authenticity. A hash-valid trace is not a publication grant.

Actual station signer/profile, immutable signed claim+closed-trace carrier and
public verification authority remain external source dependencies.
No new carrier, key client, HMAC authority, board callback or permissive issuer is
introduced. Original private `TestPublishRefusesASealedClaimWithNoAccessTrace`
remains under original custody; the new host-side missing-trace counterpart is
not a claim that the private board publication witness ran. The completion test
keeps `RequiresConfinement` true and exercises a declared durable existing lease;
pre-launch confinement/claim issuance is not qualified by that terminal-step test.
The failure cache is operation-owner memory, not a durable signed rejection record;
restarting/reissuing that canonical evidence remains outside this extraction.

Changed builder checks use the pinned SDK and admitted heavy-slot. Explicit test
names and real exits are in task evidence, without a provider/full-suite run:

```sh
go test -p 1 -parallel 1 -count 1 ./sessioncontext -run '^(TestSuccessfulBuilderRunFinalizesAndPublishes|TestFinalizeAndRequireCoverageActuallyCallsTheManager|TestCompleteMaterializationFinalizesTheBuilderGateway|TestBuilderGatewayFailedCloseCannotSucceedOnRetry|TestBuilderGatewayCancellationClosesOwnedSurfaceWithoutEvidence)$'
go build -p 1 ./sessionmanager ./sessioncontext
go vet -p 1 ./sessionmanager ./sessioncontext
```

Original gateway/trace roots are individually selected from the four new
`sessionmanager/builder_*_test.go` files in the evidence command ledger. All raw
logs, source custody maps, bounded controls and limits are outside the worktree
under a local `.temp/` evidence directory. Unix socket tests use
short, test-owned temporary directories for the platform path-length limit,
with cleanup; repository and trace fixtures remain in task-local test temp roots.

### Builder launch and independent native observation

`Service.LaunchBuilderHost(ctx, sessionView, factory, request)` implements the original
builder-side launch sequence for an **already authorized, leased** catalog entry.
It reads the actual catalog role/target/current writer, authority context/run/
target facts and the current local policy. The original building-project/profile/
producer-role predicate must agree with the trusted runtime confinement fact.
The existing typed `SessionView` must carry actual current target/run facts,
matching the catalog and authority. Its opaque host owner/session slot must
match the provider request; that owner key is never parsed as target authority.
Caller strings constrain those reads and grant nothing. Writer activity uses the original queued/running status; the compatibility
`RunEvidence.Active` member is not an additional required authority fact. It passes
the original typed plan's absolute repository, configured allowlist, produced
run, declared paths and actual private gateway endpoint to the factory.

The gateway listens before `factory.Launch`. After Launch, the service rechecks
the same lease/catalog/context/policy, validates the returned host's confinement
roots and tool-isolation reports, requires Running, and finally binds that native
observer. Binding is the last acceptance action: a rejected launch closes the
unbound runtime and cannot expose successful terminal evidence during cleanup.
Failure stops only that launch's returned host, retains the original cause, and
closes its gateway. An empty context ID preserves the ordinary factory path;
a ready context is consumed without pretending to be a new builder production.
Neither path accepts caller-injected builder confinement.

This method implements no Attach admission, canonical claim issuance or signed
publication. The core must integrate actual materialization record/lease/rollback
facts and call this method in its existing Launch transaction. Its current
MaterializationCtxID refusal remains until that separate owner work is present.
Native event fixtures use real isolated files and loopback RPC; factory-order
fixtures use declared provider reports, not a running sandbox or model. Missing
real raising/trust/reporting facts remain failclosed. The canonical
signer/profile/claim-and-trace carrier remains an external source dependency.

Changed source checks (use the pinned SDK and admitted heavy-slot):

```sh
go test -p 1 -parallel 1 -count 1 ./sessionmanager -run '^(TestCodexBuilderObservationClassifiesNativeProviderItems|TestClaudeBuilderObservationClassifiesNativeProviderBlocks|TestCodexBuilderObservationReadsOwnedNativeTurn|TestClaudeBuilderObservationReadsCapturedNativeSuffix|TestBuilderGatewayReconcilesActualNativeTranscript)$'
go test -p 1 -parallel 1 -count 1 ./sessioncontext -run '^TestBuilderLaunch'
go build -p 1 ./sessionmanager ./sessioncontext
go vet -p 1 ./sessionmanager ./sessioncontext
```

Exact commands, exits, original-body/receiver maps and bounded control results
are retained outside source in
a local `.temp/` evidence directory.
