# Work-reference input

The Codex adapter can submit an admitted task reference to the native session's
existing workspace workflow. It keeps the same private `Notice{id,reference}`
control envelope. The host does not read task contents, execute shell commands,
select a launch profile, obtain credentials or answer approval requests.

`--notice-mode synthetic` is the default and preserves the original no-tool
reply prompt. `--notice-mode work-reference` is an explicit Codex-only opt-in.
Unknown modes and work mode on a fixture adapter refuse. Vendor, model and
reasoning remain opaque strings. The native release remains pinned to 0.164.0;
a declaration in a flag does not verify the actual running executable.

Work mode additionally requires `--codex-policy yolo`, a separately admitted
permission choice. Missing, unknown or conflicting policy refuses before native
initialization. The notice mode never selects permissions on its own. Synthetic
mode rejects this option and retains its existing request fields.

## Run

First admit one isolated workspace, its actual task references and output paths,
the exact Curator profile/lock and native executable, and the original runner's
launch plan. Verify effective app-server permissions and the owned process stop
boundary for that route before real input. A yolo wrapper flag or a successful
interactive launch alone does not establish app-server thread permissions.
The current manual Codex path calls no managed home-lease/observer APIs; the
synthetic admission seam is not a substitute for production admission.

Select cooperative stop explicitly when preparing that runner:

```sh
.temp/prototype/bin/swarma-session-runner \
  --socket "$RUNNER_SOCKET" --execution "$EXECUTION" \
  --io-timeout 5s --stop-signal term < admitted-plan.json
```

The plan must name the admitted Curator route. This command is an interface
example, not evidence that a particular installed wrapper forwards termination.

With that runner already admitted and running, use absolute private socket and
state paths and the exact admitted selections below. These are shell variables
for nonsecret deployment values, not credentials:

```sh
.temp/prototype/bin/swarma-session-host serve \
  --socket "$HOST_SOCKET" --runner "$RUNNER_SOCKET" --state "$HOST_STATE" \
  --execution "$EXECUTION" --adapter codex --notice-mode work-reference \
  --codex-policy yolo \
  --workspace "$WORKSPACE" --native-release 0.164.0 \
  --vendor "$VENDOR" --model "$MODEL" --reasoning "$EFFORT" \
  --io-timeout 20s --poll-interval 25ms

.temp/prototype/bin/swarma-session-host request \
  --socket "$HOST_SOCKET" --timeout 25s < request.json
```

Status is `{"op":"status"}`. Read its actual execution and controller epoch;
do not assume an epoch after host replacement. An enqueue request has this form:

```json
{"op":"enqueue","execution":"ADMITTED_EXECUTION","epoch":1,"notice":{"id":"work-one","reference":"ADMITTED_TASK_REFERENCE"}}
```

Replace the example execution, epoch and reference with the admitted values.
References contain 1–128 ASCII letters, digits or `._:/@+-`; they are identifiers,
not task bodies or command strings. The fixed work prompt asks the native session
to resolve that reference through its existing workflow, do only authorized work
and report a missing, ambiguous or out-of-scope reference. The caller/workspace
workflow remains responsible for admitting the actual task; syntax validation is
not an authorization decision. Private same-UID sockets do not isolate agents
sharing an OS account.

## Durability and refusal

Rendering mode cannot change within an execution. Legacy host state versions 1/2
and Codex driver checkpoint version 1 mean synthetic only. Work mode uses host
state version 3 and driver checkpoint version 3, both carrying explicit mode;
the driver checkpoint and stream plan digest also bind the explicit permission
policy. The notice intent digest binds mode. Missing,
unknown or mismatched mode/version refuses before native initialization or a
runner write. The host also checks the factory's immutable `NoticeMode()`
metadata; a work factory without that declaration refuses. The shared
`adapterprotocol` version 1 interface is unchanged.

For the admitted yolo policy, the pinned adapter sends `approvalPolicy: never`
and `sandbox: danger-full-access` in `thread/start`, and repeats
`approvalPolicy: never` with `sandboxPolicy: {type: dangerFullAccess}` in
`turn/start`. It validates the thread response's approval policy, sandbox type
and exact workspace; the pinned turn response has no policy echo. The
interactive bypass flag alone does not establish this app-server policy.
No raw configuration overrides are accepted. These request/response checks
still require exact-binary effective-policy qualification before real use.
The earlier work checkpoint version 2 cannot restore into this contract.

Exact ID/reference retries retain one intent. A changed reference with the same
ID conflicts. A restart cannot turn queued or uncertain synthetic input into work,
or work into a synthetic reply. Stale epochs, hold, busy state, security stop,
uncertain receipts and output gaps retain their existing fences. A pending or
unknown write is never replayed merely because the host was replaced.

Work mode recognizes `item/started` and `item/completed` metadata for
`userMessage`, `agentMessage`, `reasoning`, `plan`, `commandExecution` and
`fileChange`, correlated to the current thread/turn. These events are progress,
not completion or permission. Their commands, patches and output are discarded.
Unsupported items, malformed identity or conflicting current-turn metadata close
input. Status exposes `state.protocol.refusal = "adapter_observation_refused"`
with an unknown fence; native error text is not persisted. Approval requests
retain a separate typed authority-required observation and are never answered.
Other tool families need qualification before use.

Only matching successful native turn completion records `outcome: completed`.
This proves a native turn ended, not that the task succeeded. Inspect the actual
requested artifact against its task and source independently. Unit/process
fixtures do not prove live tool execution or effective permissions.

## Replacement, limits and rollback

Replace only the host, keeping the original runner, native session, state path
and mode. Use the returned new epoch and a new host control socket. Before
calling this real-use retention evidence, inspect a genuine first task artifact,
verify the same admitted runner/native identities, complete a second admitted
reference, and verify owned stop. No transcript or credential export is needed.

The existing bounds remain: 32 notices, 128 runner writes, 32KiB retained output,
64KiB host state, 8KiB checkpoint and four pending 4096-byte writes. Tool output
still consumes retained runner capacity even though checkpoint metadata omits
it. Exhaustion closes input; output is not rotated. This mode is not an unattended
service, automatic launcher enrollment or process-tree confinement.

The runner defaults to `--stop-signal none`, retaining EOF-driven direct-child
stop. Opt-in `--stop-signal term` closes stdin and sends TERM once to its
original still-owned, unreaped child, serialized with the original reaper.
Unknown selectors refuse before binding a socket or spawning a child. The
runner never discovers, adopts or signals a process group or another handle.

Cooperative stop shares one attempt across concurrent requests and retries.
It waits up to one runner I/O timeout for cooperative completion, then may
force only the original unreaped child and wait another timeout for reap.
Output drain has one further timeout. Required-signal failure, cooperative
expiry, uncertain wait or drain expiry returns `stop_unknown`, retained across
retries even if forced direct-child reap succeeds. With the explicit 5s runner
timeout above, phase waits total at most 15s; the 20s host RPC and 25s request
timeouts provide margins. Deployment admission must also account for cleanup
and the complete execution window; these durations do not create an admission.

Even cooperative `stopped` proves only original direct-child completion. It
does not establish native descendant absence when wrappers spawn separate
processes. Qualify forwarding and original-child reap in the actual installed
wrapper, and native-group cleanup through the route that owns that group,
before relying on it for real use.

For rollback, close further input and preserve the prior verified binary and
private journal, including queued/unknown receipts. Stop only the admitted owned
execution through its qualified route. Select the previous binary for a new
execution/state; do not downgrade an active work journal, delete uncertainty or
replay an unknown notice. Never stop an unrelated process.

## Focused verification

Run on macOS with the repository's Go toolchain and cgo support:

```sh
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 \
  ./internal/runner ./internal/codexadapter ./internal/sessionhost
```

Tests cover default rendering, explicit work rendering, separate policy admission
and transport, missing/conflicting policy echoes, mode/checkpoint/digest
refusals, same-mode replacement, exact retries, stale epochs, hold/busy/security
stop, uncertain-write recovery, output termination and visible tool refusal.
Runner fixtures exercise real TERM delivery to an inert child, concurrent stop
and retry, forced-reap uncertainty, signal authority guards, required-signal
failure, drain expiry and invalid-selector refusal with valid controls.
These use inert owned fixtures. Actual first-use evidence remains a separate
native qualification and artifact inspection.
