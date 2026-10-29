# swarma-session-host

A small, pluggable host for agent sessions on macOS. The host manages input
intent, receipts and controller ownership; a separate runner owns one child
process and its stdio transport. Agent environments belong in adapters. Vendor,
model and reasoning selections are opaque strings, with no model catalog in
host core.

**Current scope: M2 Codex protocol prototype and M3-S synthetic admission.**
A bounded Codex 0.164.0 app-server
adapter supports synthetic no-tool notices and retained-session host replacement.
Two synthetic adapters still exercise the same core using JSON and line protocols.
Claude/Muse have interface fixtures only. This is a fresh implementation; it does
not extract or reuse the board session daemon.

The runnable prototype covers bounded input and output, private Unix sockets,
controller epochs, durable queued/unknown/applied receipts, exact retry,
changed-intent conflict, fixture input eligibility, a latched security stop,
owned-child stop and host replacement. See the
[M1 fixture contract](docs/prototype-m1.md) and the
[M2 protocol contract and limitations](docs/prototype-m2.md), and the
[M3-S admission contract](docs/prototype-m3-synthetic.md).

The next slices qualify hosted launch through Curator and the launcher, followed
by Claude Code, Muse and Linux. Hosted launch integration needs
the jointly accepted model-configuration and plan/process-proof contracts. The
fixture protocol is not a replacement for the launcher's closed plugin schema,
home leases or current admission requirements.

## Build and verify

Go 1.25.5 and Python 3 are required. The macOS process observer and race
checks also use cgo and the Apple C toolchain (Xcode Command Line Tools).
Without Darwin/cgo, the process observer explicitly reports unsupported.
Run from the repository root on macOS:

```sh
mkdir -p .temp/prototype/bin .temp/prototype/evidence
GOTOOLCHAIN=local GOWORK=off go build -p 1 -o .temp/prototype/bin/ ./cmd/...
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go vet -p 1 ./...
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 ./...
python3 .scripts/prototype-smoke.py \
  --bin-dir .temp/prototype/bin --report-dir .temp/prototype/evidence
```

The smoke starts only its own synthetic processes, records their PIDs, kills
one owned host per protocol, reconnects a new host to the same runner/child,
checks retry and stale-epoch rejection, and confirms owned-child exit. Both
protocol lanes run even if one fails. The Go process tests also interrupt the
host at dispatch and acknowledgement barriers and verify that uncertainty cannot
replay. Reports and bounded fixture logs go under
`.temp/prototype/evidence/`; temporary private socket/state directories under
`/tmp/session-smoke-*` are removed after the run. No credentials, profiles,
accounts or shared services are accessed.

## Tools

| Tool | Purpose and entry point | Outputs |
| --- | --- | --- |
| Go 1.25.5 | `go build -p 1 -o .temp/prototype/bin/ ./cmd/...`; `go vet -p 1 ./...`; `go test -p 1 -race -count=1 ./...` | Binaries in `.temp/prototype/bin/`; test results on stdout |
| gofmt | `gofmt -l cmd internal` | Paths requiring formatting on stdout |
| Python 3 | `python3 .scripts/prototype-smoke.py --bin-dir .temp/prototype/bin --report-dir .temp/prototype/evidence` | Per-lane reports and fixture logs in the explicit report directory |
| swarma-session-runner | `.temp/prototype/bin/swarma-session-runner -socket PATH -execution ID -io-timeout 1s < plan.json` | Own child/stdin/stdout and a private control socket; bounded metadata on stdout |
| swarma-session-host | `.temp/prototype/bin/swarma-session-host serve ...`; `request -socket PATH -timeout 2s < request.json` | Private state snapshots and bounded JSON responses; [complete flags](docs/prototype-m1.md#commands) |
| session-fixture | `.temp/prototype/bin/session-fixture -protocol json` or `-protocol line` | Synthetic acknowledgements on stdout |
| Codex 0.164.0 app-server | Through an admitted launch plan: `app-server --listen stdio://`; [host adapter flags](docs/prototype-m2.md#commands-and-verification) | Native stdio consumed in memory; bounded metadata/checkpoints in the private host state directory |
| Curator | `curator run codex_cli --native --profile PROFILE --model MODEL --effort EFFORT --permissions native -- app-server --listen stdio://` for an admitted disposable protocol test | Profile-managed environment and credential custody; no credential values or native transcripts in host artifacts |
| macOS process identity | `CGO_ENABLED=1 GOTOOLCHAIN=local go test -run TestOwnedKernelIdentity ./internal/admission` | Test output; reads only the owned test process in this check; no persisted native process inventory |
| Git | `git rev-parse HEAD`; `git log --show-signature -1` | Revision and signature evidence on stdout |
| Historical SH1 tools | See [archived extraction tooling](docs/sh1-gate.md) | Historical fixture/gate reports; not a prototype dependency or acceptance proof |

Prototype verification uses module-wide `go vet` and `go test` with no private
source checkout. Historical SH1 extraction results do not establish prototype
acceptance. Outstanding integration and repository-owner actions are in
[UNRESOLVED_QUESTIONS.md](UNRESOLVED_QUESTIONS.md).

[Module CI](.github/workflows/module.yml) runs Go 1.25.5 module vet and race
tests on GitHub-hosted macOS, using only this public repository. Each check records its start load and starts only below 20; cgo is explicit.
Both checks are attempted even if one fails or cannot start. Historical extraction workflows are retired.

## Design history and license

[spec/session-host.md](spec/session-host.md) distinguishes the current prototype
from the superseded extraction design. Existing [diagrams](diagrams/README.md)
and [hosted-launch notes](docs/inputs/hosted-launch-contract-r6.md) remain design
history; they do not establish implemented capabilities.

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
