# Commands and verification

These are documented interfaces to use when execution is separately authorized.
No command in this guide was run to author the skill. Paths and selection values
in examples are nonsecret placeholders.

## Build and check from the repository root

The module is `github.com/relux-works/swarma-session-host`, with Go 1.25.5
and no required dependency modules in `go.mod`. The documented validation
target is macOS; process observation and race checks need cgo and the Apple C
toolchain. Python 3 runs the synthetic smoke. Linux UID transport does not
establish Linux runtime qualification; non-Darwin/non-cgo process observation
returns `process_observer_unsupported`. [C02](evidence.md#c02)

```sh
mkdir -p .temp/prototype/bin .temp/prototype/evidence
GOTOOLCHAIN=local GOWORK=off go build -p 1 -o .temp/prototype/bin/ ./cmd/...
CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go vet -p 1 ./...
CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 ./...
python3 .scripts/prototype-smoke.py \
  --bin-dir .temp/prototype/bin --report-dir .temp/prototype/evidence
```

The three `cmd/` binaries are `swarma-session-host`, `swarma-session-runner`
and `session-fixture`. The smoke exercises both JSON/line lanes, kills its owned
first host, reconnects to the same runner/child, checks stale epochs and exact
retry/conflict, then stops owned processes. It writes `smoke.json` and per-lane
logs under the explicit report directory. It is synthetic evidence, not native
runtime qualification. [C03](evidence.md#c03)

## Synthetic run

Prepare an existing private directory with trusted ancestors; use absolute
socket paths at most 100 bytes long, with private parent permissions. State
directories must be 0700; sockets and state/lock files are 0600. Occupied socket
paths refuse rather than being removed. [C07](evidence.md#c07)

The runner plan is one JSON object on stdin. For an inert JSON child:

```json
{"binary":"/absolute/path/session-fixture","args":["-protocol","json"],"cwd":"/absolute/private/directory","env":[]}
```

Binary and cwd must be absolute, env must be an explicit non-null array, and
args are bounded to 32 entries. No parent environment is inherited.
Start runner and host in separate foreground processes. [C04](evidence.md#c04)

```sh
.temp/prototype/bin/swarma-session-runner \
  -socket /absolute/private/runner.sock -execution fixture-execution \
  -io-timeout 1s -stop-signal none < fixture-plan.json
.temp/prototype/bin/swarma-session-host serve \
  -socket /absolute/private/host-1.sock -runner /absolute/private/runner.sock \
  -state /absolute/private/state -execution fixture-execution \
  -adapter json-fixture -input-state idle \
  -vendor future-vendor -model unlisted-model -reasoning new-effort \
  -io-timeout 2s
.temp/prototype/bin/swarma-session-host request \
  -socket /absolute/private/host-1.sock -timeout 2s < request.json
```

For the line lane, use child `-protocol line` and Host `-adapter line-fixture`.
The fixture Host requires explicit `-input-state idle` to deliver; absent state
is unknown. Vendor/model/reasoning remain opaque bounded strings, with no
catalog or fallback. [C05](evidence.md#c05)

Request status with `{"op":"status"}`, then use its actual execution and epoch
in mutations. Example shape only; replace epoch 1 with the returned value:

```json
{"op":"enqueue","execution":"fixture-execution","epoch":1,"notice":{"id":"one","reference":"fixture/one"}}
```

Host operations are `status`, `enqueue`, `hold`, `release` and `stop`.
For hold/release/stop use the same execution/epoch envelope without a notice.
Wrong mutation selectors return `stale_session`. `goal.set` and `goal.clear`
return `goal_operation_unsupported`; other operations return
`unsupported_operation`. `request` needs a positive timeout no greater than
one minute, emits the response JSON, and exits nonzero on a non-`ok` code.
An `ok` enqueue response retains intent; it does not mean work completed.
[C06](evidence.md#c06) [C08](evidence.md#c08)

## Conditional Codex interface

The Host can connect to an already separately admitted runner with
`--adapter codex --workspace /absolute/admitted/workspace --native-release 0.164.0`,
explicit `--vendor`, `--model`, `--reasoning`, `--io-timeout` and
`--poll-interval` (the documented example uses 5s and 25ms). Codex refuses
`--input-state`: its state comes from protocol observations. A release flag
is a caller declaration, not executable verification. This guide provides
no admitted native launch, credentials or live-runtime route.
[C11](evidence.md#c11)

Default `--notice-mode synthetic` renders a no-tool notice.
`--notice-mode work-reference --codex-policy yolo` is an implemented, explicit
Codex-only option requiring separately admitted task/workspace and permission
custody. Mode alone grants nothing; synthetic mode rejects that policy option.
The reference grammar is `[A-Za-z0-9._:/@+-]{1,128}`, not a task body or command.
Read [the repository's work-reference contract](../../../../docs/work-reference.md)
for exact permission transport, replacement and stop limitations before assessing
that interface. Its fixture checks do not establish live tool execution.
[C12](evidence.md#c12)

## Select verification by the affected boundary

These existing tests are source-inspected only. Run only the relevant checks in
an authorized environment; use the module command above for full vet/race
validation when required. The following package/regex pairs are arguments to
`CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1`:

| Change or investigation | Package and optional filter | Source evidence |
| --- | --- | --- |
| Synthetic adapters, replay, state loss, input fence | `./internal/fixtureadapters ./internal/sessionhost`; filter `TestAcknowledgements\|TestPlugins\|TestLostAcknowledgement\|TestInputEligibility\|TestSecurityStop\|TestStateAnd\|TestMissingState\|TestPendingState` | [C08–C10](evidence.md#c08) |
| Driver contract, release/capability refusal, work mode | `./internal/adapterprotocol ./internal/codexadapter` | [C11–C13](evidence.md#c11), [C19](evidence.md#c19) |
| Output custody and replacement | `-run 'TestConsume\|TestProtocolConsume' ./internal/runner ./internal/sessionhost` | [C14](evidence.md#c14) |
| Cooperative owned stop | `-run 'TestCooperativeStop\|TestOriginalChildSignalGuards' ./internal/runner` | [C15](evidence.md#c15) |
| Notification wire, journal, stream and manager | `-run '^TestNotification\|^TestManagedNotification' ./internal/sessionhost` | [C16–C18](evidence.md#c16) |
| Synthetic admission and pre-write revocation | `-run 'TestAdmission\|TestAdmitted' ./internal/sessionhost`; also `./internal/admission` | [C21](evidence.md#c21) |
| Read-only identify, owned kernel/peer checks on Darwin/cgo | `-run 'TestIdentify\|TestPeerIdentity' ./internal/runner ./internal/localipc` | [C22](evidence.md#c22) |

Supply a filter as `-run 'REGEX'` before the packages; table backslashes are
Markdown escaping, not additional shell escapes. The inert docking helper is
a separate opt-in test executable, not a product command. Read its
[fixture contract](../../../../docs/notification-docking-fixture.md) only for
independent broker-fixture docking; its `lost-reply` case is unsupported.
[C23](evidence.md#c23)
