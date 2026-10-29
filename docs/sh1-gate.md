# SH1 public component gate

## Active workflows

[`.github/workflows/sh1-lanes.yml`](../.github/workflows/sh1-lanes.yml) runs
`public-components` on pushes, pull requests and manual dispatch. Each job checks
out this repository only, with checkout credentials disabled. It requires no
private source ref, access token or private repository checkout. Both Ubuntu and
native macOS matrix jobs are gating (`fail-fast: false`, no allowed-failure lane).
Pinned actions install Go 1.26.8 with automatic toolchain selection and workspace
mode disabled, module edits refused and setup-go caching disabled.

Each job keeps dependency verification, component build and vet, then runs the
full recursive module suites. It creates a unique mode-0700 directory under
`/tmp` for `SESSIONHOST_TEST_SOCKET_ROOT`, and uses `umask 022` for the tests.
The short root avoids Unix-socket path overflow, including on native macOS;
the umask matches executable-mode fixtures. These settings do not alter any
production server paths or test assertions. The final step removes the owned
socket root after fixture cleanup.

```sh
go mod verify
go build -p=1 ./...
go vet -p=1 ./...
umask 022
go test -p=1 -parallel=1 -count=1 ./...
```

`GOTOOLCHAIN=local`, `GOWORK=off` and `GOFLAGS=-mod=readonly` apply to every Go
step. The job deadline is 20 minutes; a timeout or any failed step is a failed
job, never a passing qualification. Go's default per-package test timeout also
applies. Neither a `-run` filter nor a test skip is added by the workflow.

## Command-selection witness and inventories

The named Python standard-library test
`ComponentSelectionTests.test_full_component_selector_covers_every_package`
reads the actual component workflow and requires the whole-module `./...`
selector in its Go test command, with uncached serial execution and no name,
skip or build-tag filter. It also inspects explicit workflow/job/step `GOFLAGS`
keys: only a simple literal `-mod=readonly` declaration is admitted. Selection
flags and unknown values (including expressions, aliases or multiline encodings)
refuse. The scanner rejects a declaration when its next nonblank, noncomment
line is deeper-indented than the key; it does not parse YAML. The named regression
`test_goflags_declarations_cannot_filter_component_suites` pairs selection and
expression refusals with literal readonly controls at all three scopes.
`test_multiline_goflags_cannot_filter_component_suites` adds plain-scalar
continuations, including across blank/comment lines, and unquoted, single-quoted
and double-quoted readonly controls at each scope. Job and step fixtures assert that
exactly one anchor was replaced and the declaration was materialized. These
tests run in the component jobs and are also discovered by the existing tooling
CI's Python test command:

```sh
python3 -m unittest discover -s tools/sh1/ci -p 'test_*.py' -v
```

The witness checks command selection and explicit `GOFLAGS` declarations only;
it does not interpret arbitrary scripts, `if` guards or `continue-on-error`.
A passing static witness does not assert that a component test ran, passed or
exercised a native provider. Its bounded negative controls substitute the former
four-test command, narrow the module selector or add a `GOFLAGS` filter in
isolated workflow fixtures; the named assertions must fail. No product test
source, name, golden or assertion is changed for those controls.
The named `test_whole_module_selector_refuses_partial_component_lists` regression
rejects the former five-component scope and explicit lists missing a composed
service. Requiring `./...` also includes future command packages without changing
the selector. This source change does not replay or qualify those full Go suites.

For a compiled inventory without executing test bodies, retain the complete
stdout/stderr and real exit of this command under `.temp/`:

```sh
go test -p=1 -parallel=1 -count=1 -list '^Test' ./...
```

The output records each selected package's top-level test names, including
recursive helper packages. Counts are platform/build-tag specific; test listing
is compilation and discovery evidence, never full-suite pass evidence. Dynamic
subtests, inactive build tags and provider behavior require execution evidence.

## Separate tooling CI and historical extraction tools

[`.github/workflows/sh1-tooling.yml`](../.github/workflows/sh1-tooling.yml)
retains its independent Ubuntu/native macOS tooling jobs with Go 1.25.5:
Python report-admission regressions, formatting, vet, race tests and build for
`tools/sh1/...`. The tooling workflow uploads its explicit fixture evidence
under `sh1-evidence` in the runner's temporary directory. The component workflow
currently exposes its commands and results in job logs; it uploads no component
archive. Local logs and scratch artifacts belong under `.temp/`.

The generic CLI and invented fixture remain historical extraction tools.
Their retained [command contract](#historical-extraction-cli-contract) below
requires caller-supplied disposable inputs and reports outside input trees.
`tools/sh1/ci/run.py` and its declarations are retained historical projection
machinery, not invoked by either active workflow. Earlier projection/consumer
reports do not qualify today's public component candidate.

## Qualification bounds

The full selector preserves test-owned native/platform, provider, resource and
opt-in prerequisites. Native macOS jobs can exercise Darwin implementations;
Linux jobs cannot qualify them by cross-compilation. Live Claude/Codex/Muse
provider tests and opt-in mutation suites remain unqualified when their declared
prerequisites are absent. A recorded skip is not a pass for the skipped behavior.
No private checkout, provider stub, renamed test, patched golden, synthetic hosted
status or local CI mirror substitutes for a required hosted check.

The assembled components do not yet constitute a complete standalone host.
Required hosted checks, independent acceptance and Curator required-green gates
remain separate obligations. Editing this workflow establishes no hosted run or
Curator acceptance; evidence must bind the exact tested commit, platform,
commands, exits and remaining unavailable subjects.

The following tool contract and evidence schema are retained for historical
extraction work. They are not prerequisites or private inputs for the active
component workflow described above.

## Historical extraction CLI contract

Build the standard-library-only CLI with Go 1.25.5 (the source module's Go
version). Paths below are relative to a disposable workspace. Keep reports outside
input trees. Every subcommand requires every documented flag, returns 0 on success
and 1 on refusal, and writes stable indented JSON with a trailing newline.

```sh
go build -o sh1 ./tools/sh1
./sh1 generate -source source -package internal/session \
  -old example.org/sh1-fixture/internal/session -new example.org/sh1-host \
  -mod candidate-go.mod -out candidate
./sh1 project -candidate candidate -checkout projection \
  -package internal/session -commit "$SOURCE_COMMIT" \
  -adapters adapters.json -report evidence/project.json
go -C projection test -json -count=1 ./... > evidence/tests.jsonl
./sh1 inventory -source projection -events evidence/tests.jsonl \
  -guards guards.json -report evidence/inventory.json
./sh1 mutate -checkout projection -mutations mutations.json \
  -report evidence/mutations.json -- go test -json -count=1 ./...
./sh1 consumer -candidate candidate -checkout consumer \
  -version v0.0.0-20261010000000-0123456789ab \
  -proxy proxy -report evidence/consumer.json
```

`candidate-go.mod` is a reviewed complete module file declaring the new module
and the same Go version as the source. Its dependencies are explicit caller
inputs; the generator does not guess neutral interfaces, copy unrelated internal
packages, or resolve dependency versions. Candidate replace directives are
refused. Source modules may have unrelated replace directives; none is copied
implicitly. Unsupported dependency graphs require a design decision at F.

The CLI accepts lowercase module paths with a dotted domain and ASCII letters,
digits, `._-/`; v0 pseudo-versions are the supported initial proxy interface.
Relative file declarations must be normalized, nonempty, and remain inside their
tree. Symlinks and other nonregular files are refused. Inputs and outputs are
trusted local workspaces, used serially with no concurrent writers. This tool
is not a security boundary for an adversarial filesystem or forged manifests.

### generate

Copies every file recursively, including tests, fixture programs, testdata and
non-Go files. Rewrites only Go AST import literals matching the old path exactly
or its slash-prefixed descendants; comments, string data, and file names stay
unchanged. Reserved root `go.mod` and `sh1-manifest.json` collisions are refused;
the output must be absent or empty. A supplied candidate module file is emitted
as `go.mod`.

`sh1-manifest.json` lists each output file, mode, original SHA-256, transformed
SHA-256, source go.mod digest, and protected-file classification. The supplied go.mod has no original
package hash. The manifest cannot hash itself and is excluded from its own file
list. The generator digest hashes sorted names and SHA-256s of its embedded Go
sources (including tool tests). The transform digest binds that digest, package,
old and new imports, source go.mod digest and supplied module-file digest. Local
absolute paths and timestamps are excluded. Hosted evidence must bind the
manifest hash to authenticated source/candidate identities; a self-authored
manifest alone does not establish provenance.

### project

Requires an exact 40-character source commit and a clean Git checkout. Verifies
candidate bytes, file set, modes and protected classifications against the
manifest, and original package bytes against original hashes before writes.
Maps candidate production imports back to the old path, writes production files
(including non-Go production assets), and leaves `_test.go` and every `testdata/`
file untouched. Before/after hashes of protected files cover the whole checkout.
The report records the source commit, candidate-manifest hash, adapter-table
hash, production transformations, and protected-file hashes.

The only admitted adapter table is `{"version":1,"entries":[]}`. Unknown
versions, omitted entries, and any nonempty adapter table are refused. Future
adapters require a reviewed versioned implementation, not implicit patches.

### inventory

AST discovery covers named top-level Go Test declarations in all `_test.go`
files, including inactive build-tagged files. Package rows list discovered
source tests and observed dynamic subtests, build-tag expressions, opt-in
conditions, execution flags, terminal results and skip reasons. Every skipped
test has a reason; absent or diagnostic-only output is recorded as `unknown`,
including streams without any test output events. Excluded tests
are `not_observed`; this does not infer that they passed or why they were absent.
Go list output is unnecessary for this fixture because source discovery also
sees inactive files. This interface inventories tests, not benchmarks, fuzz
corpus cases, or external packages whose source is outside the checkout.

The input is one uncached `go test -json -count=1` stream, with terminal package
and started-test events. A shared parser validates package/test lifecycles for
both inventory and mutate: one run and exactly one terminal per observed test;
no duplicate or conflicting terminals or unfinished tests at package termination.
Missing, empty, malformed, repeated, partial or
unsupported events refuse. A failed test is recorded as failed; inventory is not
a substitute for checking the original go test exit code. Build failures whose
new event actions are unsupported refuse rather than report absence.

Guard declarations bind package/test pairs to expected relative source targets.
A reviewed guard test emits `SH1_GUARD relative-file` after a successful read.
Only a marker from that named test with a passing terminal event counts.
`guard_ratio` and per-guard ratios measure reached/expected targets; incomplete
coverage writes a report and exits 1. The report does not infer coverage from a
passing guard or from a marker emitted by another test. Bound: markers depend
on reviewed instrumentation; uninstrumented guards remain unproven, and callers
must supply authenticated logs rather than forged event streams.

Opt-in declarations are explicit; the tool does not pretend to infer all
environment-sensitive behavior from syntax. The public fixture reports a
normal opt-in skip, then separately exercises the opt-in and build-tagged tests.
Lane 1 uses no name masks. The 2026-10-10 rework decision permits the board's intentional skips only when baseline and projection agree on the exact test and reason; missing or unknown outcomes refuse.

### mutate

A version-1 declaration lists unique names, relative files, exact unique anchors,
replacements, and required package/test pairs. Runs the caller's command once as
a baseline and requires exit 0 and every named test passing in JSON evidence.
Each mutation is applied independently, the identical command is run, and the
file is restored before the next mutation or any reported command error.

A kill requires a nonzero command exit, exactly one completed named test
execution with a failure terminal, and its package failure. Baseline admission
requires exactly one completed passing execution of every named test. Compile
failures, malformed logs, another test failing, and
survivors do not count as kills. Reports retain actual exit codes, before/after
hashes, killed/survived/unknown statuses and a measured kill ratio. Any surviving
or unknown mutation exits 1 after writing the report. The fixture proves both
narrowing the admitted identity class and bypassing its gate, plus a by-name
source guard. Later gates declare converter and additional real guard mutations too.
Caller commands must emit Go test JSON on stdout; diagnostics belong on stderr.
The CLI forwards every baseline and mutant stream to stdout, including refused
baselines, and labels each stage and its actual exit on stderr. This stream is
retained in public job logs, excluded from artifacts; it is not one inventory
stream because each stage repeats executions.

### consumer

Validates the candidate manifest, serves sorted deterministic `.mod`, `.info`
and `.zip` bytes from `module/@v/` plus a version list, then edits the consumer's
require with Go's module editor and rewrites matching AST imports throughout
its Go files. It never adds a replace and refuses an existing replacement of
the candidate module. Existing unrelated replacements remain caller-owned.
Conflicting bytes at a published proxy version are refused.

Runs effective module selection, download and module graph commands with a
fresh module cache, GOWORK off, only this file proxy, and the checksum server disabled for the local synthetic
publication. Records command exit codes, the two candidate go.sum lines, the
module graph, proxy file hashes and consumer file hashes. Selection from
`go list -mod=mod -m -json all` must equal the requested pseudo-version; a higher
MVS minimum refuses. Checksums must have exactly the candidate zip and `/go.mod` keys for
that version and match `go mod download -json`. Historical and unrelated
checksum lines are preserved and excluded from the candidate checksum pair.
A v0 pseudo-version labels a disposable candidate; the synthetic timestamp/revision in the example
is not an attestation of a real release. Hosted workflows derive those values
from the pinned candidate commit. Later release gates use immutable tags and
normal release checksum policy.

The caller prepares the consumer at dependency exit: the extracted package is
absent, while consumer composition and command sources remain. The fixture
removes its invented extracted package and tests both the parent and child
through the cmd consumer. Original extracted tests run in lanes 1 and 3. All
consumer dependencies must be available in the local proxy or via existing
reviewed unrelated module resolutions; the tool does not silently fall back to
the network. Run the real consumer build and full test commands with the same
GOPROXY after this preparation step. The public end-to-end test does so.

## Historical extraction evidence schema

The gate's evidence bundle must include:

- Source commit, source go.mod and recursive source file hashes; candidate commit,
  host tree, candidate module file, candidate manifest and transform digest.
- Generator source digest; workflow, adapter table, helper, test, guard declaration,
  mutation declaration, fixture and golden hashes. Record an explicit empty
  adapter set rather than omit it.
- Exact commands and exit codes, source discovery and runtime inventories,
  skip reasons, active/inactive tags, guard reached/expected ratios and each
  mutation's named-test outcome. No truncated logs or inferred passes.
- Consumer require, candidate module version, go.sum lines, module graph and proxy
  publication hashes; later the immutable release tag and authenticated identity.
- Go version, GOOS, GOARCH, relevant build tags and opt-in environment values,
  runner class, toolchain policy, workflow/run identity and per-job durations.
  Never record secrets, personal paths, private machine names or session links.

Artifacts live with the run and are attached to the Change Request before
handoff. F remains provisional until the actual copy snapshot closes and all
three lanes run on those pinned bytes.

## Archived extraction evidence (superseded)

The reports below are retained verbatim as historical evidence. Their lane
descriptions, toolchains, private source inputs, qualifiers and references to
"current" describe the earlier recorded runs only. Those workflows have been
removed; none of these reports qualifies the present component selector, its
Ubuntu/native macOS jobs or the standalone host. The active contract is above.

## Baseline-equivalence rework evidence

This worker triggered and collected fresh [lane run 38033458412](https://github.com/relux-works/swarma-session-host/actions/runs/38033458412)
and [tooling run 38033458450](https://github.com/relux-works/swarma-session-host/actions/runs/38033458450).
They use signed public snapshot `c078a21482724d7cb3b264c7449a26846f864b0a`
(tree `2f24cfe144d074a44a491565114b3b1d40c0564f`), with board main resolved once to
the pinned private source commit recorded in the private fork's provenance record. The Story working tree remains
uncommitted. All 29 validation input files match the tested snapshot byte-for-byte;
only this evidence documentation changes after execution. Historical strict-policy
runs below are inherited evidence, not baseline-equivalence proof. This is the
current-main development gate; the Muse-cut freeze remains a separate task.

| Identity | SHA-256 |
| --- | --- |
| Generator | `97501ea8ccfeb9427961ff68c316aa27e6a56f037ddb241ac78f56702c6db752` |
| Manifest | `a0fa185a8135d5a63085f296ec8337b1472f086ce73891277c413df93fc445c7` |
| Transform | `d678d4b3921722416f04a2a149268b9597d08e2b95326ec00d3e2736666500f9` |
| Source go.mod | `b06d4da806e5337faf6f1485b13a6676e16fe60f3c1b4ab62758d64f7c832f60` |
| Candidate go.mod | `7622e5ff5ec276bcf97f3554dd65d531838d266c1e71444f3d3ab4b29a3ab6d4` |
| Workflow | `86dd51cbaa83d9310c66d6f5ecfe826ba053633f9c4a58d070e71403f228570a` |
| Lane helper | `1a5adeb371f19fc373fb11978ff54f6d7048b5b0184e9a6943779f2198f922e1` |
| Helper regression tests | `ddbc4c18bfa01c94d54714eafc43f80be85c7818e506eb74f98d6aef6e3ea333` |
| Empty adapter declaration | `e5a76fe3d134f87e636debc55cc15ac01dbd2bbf0ed1edac80c0f3b35a228eb3` |
| Mutation declaration | `531deb1fb41299dc8efbe9c31787c30f686b5bcf931cb71ed139f38e91fe83f8` |

The manifest has 290 files including the supplied module file, with 195 protected
package files. The projection report binds 94 production files and 1,366 protected
files across the board's CLI module to the pinned private source commit. Every before/after hash is
equal. Recursive inventory retains nine unselected MSP fake-server fixture
declarations outside the unchanged two-package command; these remain compared
as unobserved. Both package commands use Go 1.25.5 from board go.mod, GOTOOLCHAIN
local, GOWORK off and no name masks. Ubuntu is linux/amd64; macOS is darwin/arm64.

| Inventory (baseline and projection agree) | macOS | Ubuntu |
| --- | --- | --- |
| Discovered declarations | 1350 | 1350 |
| Dynamic subtests | 3742 | 3731 |
| Executed | 5088 | 5073 |
| Skipped with reasons | 34 | 36 |
| Failed | 0 | 7 |
| Tagged | 60 | 60 |
| Explicit opt-ins | 3 | 3 |
| Unobserved tagged in tested packages | 4 | 8 |
| Unselected fixture declarations | 9 | 9 |

macOS compares 5,104 outcomes including package rows; 0 differences. Baseline test exit 0 (358.185 s), projection test exit 0 (318.509 s).
Ubuntu compares 5,093 outcomes including package rows; 0 differences. Baseline test exit 1 (304.19 s), projection test exit 1 (269.535 s).

Both inventory CLI commands on each platform exit **1** for informative guard
read coverage **0/10, not instrumented upstream**; these are real expected-red
commands, not passing inventory gates. The valid reports and comparison remain
usable. Coverage is 0/9 for `TestClaudeContextProductionHasNoFilesystemRemoval`
and 0/1 for `TestTheMutantInventoryCoversEveryPredicateOnTheRefusalPath`. The
upstream marker request above names the successful-read instrumentation needed.
No markers, skips, masks or test patches are introduced by this workflow.

Ubuntu's seven failures occur identically in baseline and projection and are
therefore **pre-existing on Linux at this board commit**, not a projection gap:

- `TestMuseR31StopReportsUnrequestedSignalDeath`
- `TestMuseR31StopReportsUnrequestedSignalDeath/abrt_after_stdin_eof`
- `TestMuseR31StopReportsUnrequestedSignalDeath/control_clean_exit`
- `TestMuseR31StopReportsUnrequestedSignalDeath/control_exit3`
- `TestMuseR31StopReportsUnrequestedSignalDeath/segv_after_stdin_eof`
- `TestMuseR31StopReportsUnrequestedSignalDeath/self_kill_after_stdin_eof`
- `TestMuseR31StopReportsUnrequestedSignalDeath/term_after_stdin_eof`

The diagnostic is `initialize Muse MSP session: MSP server exited: EOF`. No
Linux mutation run is claimed: the declared mutations gate on macOS. Intentional
provider/helper skips and Darwin-only Linux exclusions have identical names and
reasons in the paired inventories; the observed reasons remain in both uploaded
inventories. Package failures are retained in full JSON streams in public logs.

macOS `mutate` exits 0 in 1262.297 s; its full-suite baseline exits 0. Measured named kills: **3/3**. The CLI retains the baseline and every mutant stdout stream, with stage exit labels on stderr.

| Mutation | Required failing test | Mutant exit | Result |
| --- | --- | --- | --- |
| `local-key-isolation` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |
| `remote-default-port-normalization` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |
| `remote-board-identity` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |

Mutant test commands exit 1 deliberately; they are not green package tests.
All six complete full-suite JSON streams are preserved in the public log
(**6/6**, no partial stream). The board baseline, projection and mutation
baseline have no failing tests. Each mutant fails only
`TestBoardKeysCanonicalizeAndIsolateBoards`; there are no additional mutant
failures. Extracted terminal metadata is retained in the task outcome.

| Report | macOS SHA-256 | Ubuntu SHA-256 |
| --- | --- | --- |
| `project.json` | `eabc2b7608c2091d5a84fb98f70ced702f05e914388ef8b843714395258db129` | `eabc2b7608c2091d5a84fb98f70ced702f05e914388ef8b843714395258db129` |
| `baseline-inventory.json` | `c4fbb712293c0c33915b35c7c2ce31566d4d8a5e9a23f91042fa4be46e81ad3f` | `e9ca6ff2a9336a5f16776641caf37db76c5fe588595890f899b5ef0294103b7a` |
| `inventory.json` | `6061795f9794857bd84c792d539a2cd30df5a3aed79c927e1e3f9a4b3ab53854` | `dc11555c5cf94b7ddb9a09af884adc1a3a5fca30ce82055affc3121bf4d7ab84` |
| `equivalence.json` | `559939eb822489adc2cb4a8c029800dfdd5c8321eac24a867e4ae2cf623dc1fd` | `1ba19b84aaae2127d94bf571385c0701c0ed6b0ebed06d9734a2a86a24d1e458` |
| `guards.json` | `33233625f3fce7807ea4d1a798c7cd5b27281ce98d8ebd5775922f7bea55662f` | `33233625f3fce7807ea4d1a798c7cd5b27281ce98d8ebd5775922f7bea55662f` |
| `mutations.json` | `5d2413869aac5bab49a3542d36d7c12d8270bc6f4d2642d4c15d3ea1a4e29ec0` | `not run: macOS only` |

| Hosted job | Duration | Actual result |
| --- | --- | --- |
| source | 5 s | success |
| consumer (ubuntu-latest, gating) | 85 s | success |
| projection (ubuntu-latest, informative) | 622 s | success |
| projection (macos-latest, gating) | 2031 s | success |
| tooling (macos-latest) | 51 s | success |
| tooling (ubuntu-latest) | 43 s | success |

Helper durations: Ubuntu projection 580.995 s; consumer 45.127 s; macOS projection 1949.838 s.

Tooling passes on both platforms: **11 Python regression tests**, the whole Go
race/refusal/fixture suite, formatting, vet and build, each exit **0**. The new
regressions drive `Lane.projection` and the real `mutate` CLI, refusing changed
outcomes/skip reasons, disappearing tests, unknown/missing inventories, an
incorrect testdata scope and lost failed-baseline stdout. The existing lifecycle
narrowing experiment also passes. No test/build/vet ran on the development host.

Consumer preparation, `go build ./...` (25.137 s) and `go vet ./...`
(12.169 s) each exit **0**. Selection/download/graph commands also exit 0.
The file proxy selects `v0.0.0-20261010070938-c078a2148272` with these two candidate lines:

```text
github.com/relux-works/swarma-session-host v0.0.0-20261010070938-c078a2148272 h1:+I1NExvmBiFYx2g/5p2WY6agxlYuAnJAuawARIXtW1U=
github.com/relux-works/swarma-session-host v0.0.0-20261010070938-c078a2148272/go.mod h1:+/vQzHkzfzWcTK+Tiirk8mscTIpoOkr6eEc0K2yvpfk=
```

`consumer.json` SHA-256 is `e5e98e55947e5c6d7759f90e861e1a6efc6e2fc2835228328b11e0528bf53012`,
with 60 module-graph rows and all proxy hashes. No candidate replacement is
present; two unrelated retained-library replacements keep their documented
prototype role. Tests needing board-internal access remain in lane 1. Artifacts
contain only allowlisted JSON reports, never board/candidate/proxy/cache bytes
or raw logs. Outcomes `TASK-261010-3uprc3_rework-hosted-reports.json` and
`TASK-261010-3uprc3_rework-logbook.md` retain all reports and every failure.

The first rework attempt [38032951891](https://github.com/relux-works/swarma-session-host/actions/runs/38032951891)
used signed snapshot `b97f1a5c5edd78349318d5b4f110e97d057e9b4c`
(tree `0f8ee64f9d0fa832f1b4282427b94baf804a72c5`) and the same board commit,
generator and candidate bytes. It refused after baseline inventory because the
checker incorrectly rejected nine unselected fixture declarations; both helper
exits were **1**. No projection or mutation executed; equivalence and kills
were unproven. macOS baseline exited **1** with
`TestMuseStrictMalformedChildCannotSucceed` reporting fake-host cleanup failures
(already-closed pipes and `signal: killed`); Ubuntu baseline exited **1** with
the same seven signal-death failures named above. macOS helper duration was
371.428 s, Ubuntu 308.516 s. The checker scope is corrected with a new regression;
no board test was changed. Its consumer and [tooling run 38032951924](https://github.com/relux-works/swarma-session-host/actions/runs/38032951924)
passed; tooling had 10 Python regressions then. These failing baseline streams
and inventories remain attached, not reclassified as passing evidence.

## Historical lane 1 and 2 evidence (superseded admission policy)

Hosted verification uses the pinned private source commit of board main, recorded
in the private fork's provenance record. The tested public
snapshot is `569e0087de83d45a344a59c6446eeff602a20512` (tree `415ef2531002a3fae6be3222822ea30a3a92a814`); subsequent worktree changes
only add evidence documentation. The generator and candidate bytes are equal
across all three snapshots. The manifest has 290 files including the supplied
module file, with 195 protected package files. Projection compares 94 production
files and 1,355 protected files across the board's CLI module: every before/after hash is equal.

| Identity | SHA-256 |
| --- | --- |
| Generator | `b0f8c2d2af6fe914bab751734a30e417a8ac43ec9fb446ed5c000e280fcd4b38` |
| Manifest | `cae78e6e843b57935164cc60e9bb9f92ab8875da91a69b98da07059be2cfe457` |
| Transform | `59529bf2650cc15475236889fec8ce2d981b9240013ae8a2342c5edeeaae124a` |
| Source go.mod | `b06d4da806e5337faf6f1485b13a6676e16fe60f3c1b4ab62758d64f7c832f60` |
| Candidate go.mod | `7622e5ff5ec276bcf97f3554dd65d531838d266c1e71444f3d3ab4b29a3ab6d4` |

The consumer selected `v0.0.0-20261010054135-569e0087de83` through the file proxy, with
these candidate checksums (also in `consumer.json`):

```text
github.com/relux-works/swarma-session-host v0.0.0-20261010054135-569e0087de83 h1:4D342Kf+T2XZFL9SUWYyxKa70MAXgFdqBkQMOiM5nS0=
github.com/relux-works/swarma-session-host v0.0.0-20261010054135-569e0087de83/go.mod h1:+/vQzHkzfzWcTK+Tiirk8mscTIpoOkr6eEc0K2yvpfk=
```

`consumer.json` records 60 sorted module-graph rows and every proxy and
consumer file hash; its SHA-256 is `06ae39798e733efe4853ef0f2e797f6daeb65ba23ddd96a9e36ce02f321532a5`. No replacement of the candidate
module is present. The board's two original, unrelated library replacements
remain documented in the hosted lane contract; this is a prototype retained
dependency graph, not dependency-exit or released-module proof.

All command exits are in the task outcome `TASK-261010-3uprc3_hosted-reports.json` and run JSON artifacts. The tooling commands `python3 -m unittest discover -s tools/sh1/ci -p 'test_*.py' -v`, format checks, `go vet ./tools/sh1/...`, `go test -v -race -count=1 ./tools/sh1/...` and `go build ./tools/sh1/...` exit 0 on both platforms for snapshot 3; the helper suite has eight tests. Its malformed-event child intentionally exits 7, and the regression verifies that exit is retained. Consumer proxy preparation, `go build ./...` and `go vet ./...` exit 0 for all three snapshots. No test, build or vet command runs on the development host.

| Run / snapshot | macOS projection | Ubuntu projection | Ubuntu consumer | Tooling Ubuntu / macOS |
| --- | --- | --- | --- | --- |
| [38027685088](https://github.com/relux-works/swarma-session-host/actions/runs/38027685088) / `545a4c6583be` | cancelled, 872 s | failure, 597 s | success, 85 s | [both pass](https://github.com/relux-works/swarma-session-host/actions/runs/38027685094), 32 s / 62 s |
| [38027807183](https://github.com/relux-works/swarma-session-host/actions/runs/38027807183) / `f533a3ce1350` | cancelled, 753 s | failure, 571 s | success, 89 s | [both pass](https://github.com/relux-works/swarma-session-host/actions/runs/38027807133), 40 s / 49 s |
| [38028369569](https://github.com/relux-works/swarma-session-host/actions/runs/38028369569) / `569e0087de83` | failure, 1689 s | failure, 610 s | success, 89 s | [both pass](https://github.com/relux-works/swarma-session-host/actions/runs/38028369582), 38 s / 42 s |

The first two macOS jobs were cancelled because their inventory scope was
superseded. Each full suite had exited 0 with 5,088 run events and 34 skips;
`inventory` exited 1 (`duplicate declared test` in unrelated CLI packages).
Their mutation command exits and kills are unknown because cancellation
interrupted execution before its report. Partial manifests/projection reports
and sanitized authenticated-job-log observations are retained. Ubuntu completed
in both old runs: package tests exit 1, inventory exits 1 for that same scope
refusal, and mutate exits 1 because its baseline exits 1. Failing baseline JSON
is not retained by the current CLI; its exact failing test names are unknown and
are not inferred from the preceding suite. No baseline failure counts as a kill.

Snapshot 3 scopes inventory to both tested packages. On Ubuntu it records:
1,350 discovered declarations, 3,731 dynamic subtests, 5,073 executions, 36 skips,
7 failures, 60 tagged rows, 3 explicit opt-ins and 8 unobserved tagged declarations.
The package command exits 1; inventory writes its report and exits 1 for guard
coverage. Mutation baseline exits 1; zero of three declared mutations execute
and their named-kill proof remains unknown. macOS records 1,350 discovered declarations, 3,742 dynamic subtests, 5,088
executions, 34 skips, no ordinary test failures, 60 tagged rows, 3 explicit
opt-ins and 4 unobserved tagged declarations. Its full package command exits 0,
its inventory writes the report and exits 1 for guard coverage, and its mutation
baseline exits 0. `mutate` exits 0 and proves **3/3 named kills**:

| Mutation | Required failing test | Mutant command exit | Result |
| --- | --- | --- | --- |
| `local-key-isolation` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |
| `remote-default-port-normalization` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |
| `remote-board-identity` | `TestBoardKeysCanonicalizeAndIsolateBoards` | 1 | killed |

Those mutant command exits are deliberate failures, not passing test commands.
Both projection helper processes exit 1 because their strict admission gates
reject the recorded skips and unproven guard coverage. The macOS helper takes
1,609.289 s (the package command 335.825 s and mutate 1,264.421 s); the Ubuntu
helper takes 568.706 s (package command 296.904 s and mutate refusal 265.009 s).
The consumer helper takes 44.635 s. Full job durations above include setup and upload.

| Report | macOS SHA-256 | Ubuntu SHA-256 |
| --- | --- | --- |
| `project.json` | `d759947dada8daa5aa3dd0d33854cae801307984671e9bfb4585dbfa123a96f7` | `d759947dada8daa5aa3dd0d33854cae801307984671e9bfb4585dbfa123a96f7` |
| `inventory.json` | `e063f5bfe08ae7b2e9b800ce92efe151c41f729a79689c0634b299b3e31a7e62` | `edd0d4f4c9328d5ccec96cefece91bf38413eb89434c397c5330906a411f9589` |
| `guards.json` | `f32bc0ed5a578ad0d26b215db19a07f62aeab06512641bc59d7923cacdd5e88c` | `f32bc0ed5a578ad0d26b215db19a07f62aeab06512641bc59d7923cacdd5e88c` |
| `mutations.json` | `5d2413869aac5bab49a3542d36d7c12d8270bc6f4d2642d4c15d3ea1a4e29ec0` | `unknown: baseline refused before report` |

Guard targets reached/expected on both platforms are **0/10**: **0/9** for
`TestClaudeContextProductionHasNoFilesystemRemoval` (`claude_assay_schema.go`,
`claude_context.go`, `claude_context_assay.go`, `claude_context_fork.go`,
`claude_context_lease.go`, `claude_context_reclamation.go`, `claude_host.go`,
`claude_output.go`, `claude_transcript.go`) and **0/1** for
`TestTheMutantInventoryCoversEveryPredicateOnTheRefusalPath`
(`hosted_takeover_guard.go`). No successful read markers are observed.

Every ordinary suite failure on Ubuntu in the recorded runs is the parent
`TestMuseR31StopReportsUnrequestedSignalDeath` and these six subtests:

- `control_clean_exit`
- `control_exit3`
- `segv_after_stdin_eof`
- `abrt_after_stdin_eof`
- `term_after_stdin_eof`
- `self_kill_after_stdin_eof`

Each fails during fake Muse launch with `initialize Muse MSP session: MSP server
exited: EOF`. The protected and production hashes prove the projected bytes
are identical to board main. The wrapper launches a background child with stdin;
a proposed platform adapter is upstream review of original stdin preservation
across `/bin/sh` background launch on Linux. This is a diagnostic hypothesis,
not an established root cause; no shell replacement or test patch is applied here.

Under the original policy, the task was blocked and the gate was red. The original blocker packet requested qualified provider
artifacts and trusted project inputs, a contract for intentional helper skips and
Darwin-only confinement tests on Linux, and reviewed upstream guard read markers.
The board owner must resolve the Muse fixture failure in an accepted upstream
revision. The orchestrator subsequently resolved admission through baseline equivalence,
informative Ubuntu and informative guard coverage, as specified above.
These runs do not gate the Muse-cut freeze or released-module acceptance.

The complete observed skip list below comes from the corrected Ubuntu inventory;
macOS observes the same list except the two Darwin-confinement exclusions.
All raw logs remain in GitHub; artifacts and board outcomes contain report
metadata only. ZIP attachment to the board was refused (`resource_binary_refused`),
so outcome evidence is stored as plain-text JSON instead.

| Package | Skipped test | Observed reason |
| --- | --- | --- |
| root | `TestBuilderConfinementProviderLiveSmoke` | builder_confinement_live_smoke_test.go:25: set TASK_BOARD_BUILDER_CONFINEMENT_LIVE_SMOKE=1 to run real providers |
| root | `TestBuilderToolIsolationAdversarialLiveSmoke` | builder_tool_isolation_live_smoke_test.go:97: set TASK_BOARD_BUILDER_ISOLATION_LIVE_SMOKE=1 to run real providers |
| root | `TestClaudeAdoptionLiveSmoke` | claude_live_smoke_test.go:138: set TASK_BOARD_CLAUDE_LIVE_SMOKE=1 to run the live Claude adoption smoke test |
| root | `TestClaudeContextAssayLiveSmoke` | claude_context_live_smoke_test.go:457: set TASK_BOARD_CLAUDE_CONTEXT_LIVE_SMOKE=1 to run the live Claude context smoke test |
| root | `TestClaudeContextForkLiveSmoke` | claude_context_live_smoke_test.go:34: set TASK_BOARD_CLAUDE_CONTEXT_LIVE_SMOKE=1 to run the live Claude context smoke test |
| root | `TestClaudeHostContextForkLiveSmoke` | claude_context_live_smoke_test.go:198: set TASK_BOARD_CLAUDE_CONTEXT_LIVE_SMOKE=1 to run the live Claude context smoke test |
| root | `TestClaudeHostLaunchesBuilderProcessUnderReadConfinement` | claude_host_test.go:294: production builder confinement currently uses Darwin Seatbelt |
| root | `TestClaudeHostLiveSmoke` | claude_live_smoke_test.go:30: set TASK_BOARD_CLAUDE_LIVE_SMOKE=1 to run the live Claude smoke test |
| root | `TestCodexAdoptionLiveSmoke` | codex_adoption_live_smoke_test.go:22: set TASK_BOARD_CODEX_ADOPTION_LIVE_SMOKE=1 to run the real Codex thread-adoption smoke |
| root | `TestCodexBuilderConfigurationIsAcceptedByRealCodex` | builder_tool_isolation_live_smoke_test.go:576: set TASK_BOARD_BUILDER_ISOLATION_LIVE_SMOKE=1 to ask the installed Codex to parse |
| root | `TestCodexContextForkLiveSmoke` | codex_context_live_smoke_test.go:22: set TASK_BOARD_CODEX_CONTEXT_LIVE_SMOKE=1 to run the real app-server thread/fork smoke |
| root | `TestCodexEntrypointParityLiveSmoke` | codex_entrypoint_parity_live_smoke_test.go:74: set TASK_BOARD_CODEX_ENTRYPOINT_PARITY_LIVE_SMOKE=1 to run the real two-entry-point parity smoke |
| root | `TestCodexExternalHomeAdoptionLiveSmoke` | codex_adoption_live_smoke_test.go:26: set TASK_BOARD_CODEX_ADOPTION_LIVE_SMOKE=1 to run the real Codex thread-adoption smoke |
| root | `TestCodexHostLaunchesBuilderProcessUnderReadConfinement` | codex_host_test.go:1487: production builder confinement currently uses Darwin Seatbelt |
| root | `TestCodexSpawnedOwnerLiveSmoke` | codex_host_test.go:1701: set TASK_BOARD_LIVE_CODEX=1 to run the real managed spawned-owner smoke |
| root | `TestCodexTrustAppServerHelper` | codex_host_test.go:1380: helper process |
| root | `TestMuseApplyImmediatelyAfterPreviousRevisionUsesSet` | muse_host_test.go:2195: set TASK_BOARD_MUSE_SMOKE_BIN to the copied Muse 1.4.1-R4503.1 binary to run the immediate-revision smoke |
| root | `TestMuseMSPSmoke` | msp_smoke_test.go:179: set TASK_BOARD_MUSE_SMOKE_BIN to a copied qualified Muse binary (1.4.1-R4503.1 or 1.4.2-R4684.1) to run the offline smoke |
| root | `TestMusePinnedSchemaExport` | muse_strict_decode_test.go:420: consumed closure: 28 definitions; muse_strict_decode_test.go:424: TASK_BOARD_MUSE_SMOKE_BIN is absent; byte-equality with a fresh export is unrun |
| root | `TestMuseProviderHostEchoSmoke` | muse_host_test.go:2300: set TASK_BOARD_MUSE_SMOKE_BIN to the copied Muse 1.4.1-R4503.1 binary to run the offline echo smoke |
| root | `TestMuseR23RealModelApprovalCapture` | muse_revision23_test.go:1007: set TASK_BOARD_MUSE_SMOKE_BIN to the copied Muse 1.4.1-R4503.1 binary to run the N3 capture |
| root | `TestMuseRealBinaryDowngradedResumeFailsClosed` | muse_restore_history_test.go:857: set TASK_BOARD_MUSE_SMOKE_BIN to run this real-binary Muse 1.4.1 proxy sweep |
| root | `TestMuseRealBinaryReplayAfterCancelledApplyUsesSameCommandID` | muse_revision08_test.go:705: set TASK_BOARD_MUSE_SMOKE_BIN to run this real-binary Muse 1.4.1 proxy sweep |
| root | `TestMuseRealBinaryResumeDowngradeAndRev6StaleLifecycle` | muse_revision08_test.go:745: set TASK_BOARD_MUSE_SMOKE_BIN to run this real-binary Muse 1.4.1 proxy sweep |
| root | `TestMuseRealBinaryResumeFoldsServedChildSpend` | muse_restore_history_test.go:800: set TASK_BOARD_MUSE_SMOKE_BIN to run this real-binary Muse 1.4.1 proxy sweep |
| root | `TestMuseRealProviderBudgetPause` | muse_real_provider_test.go:74: real-provider test is disabled in CI |
| root | `TestMuseTextEchoSmoke` | muse_text_goals_test.go:306: pinned echo binary not supplied |
| /musepty | `TestMusePTYR10RealLongIdleTranscriptDelivers` | muse_pty_r10_screen_test.go:188: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYR14RealQuotedChromeTranscriptDelivers` | muse_pty_r14_regression_test.go:136: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYR15RealLongQuotedHeaderDelivers` | muse_pty_r15_regression_test.go:118: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYR15RealScrolledQuotedHeaderDelivers` | muse_pty_r15_regression_test.go:140: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYReal141EchoSmoke` | muse_pty_smoke_test.go:18: TASK_BOARD_MUSE_SMOKE_BIN not provided; real 1.4.1 smoke unrun |
| /musepty | `TestMusePTYRealCompletedTranscriptAndLiteralFollowup` | muse_pty_regression_test.go:364: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYRealLiteralRuleRoundTrip` | muse_pty_grammar_test.go:267: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYRealPermissionsModalNeverPastes` | muse_pty_regression_test.go:417: pinned Echo binary not supplied; real regression unrun |
| /musepty | `TestMusePTYRealQueueDrainRestoresNotice` | muse_pty_regression_test.go:383: pinned Echo binary not supplied; real regression unrun |

## Public verification and durations

The four historical part-1 runs below are retained from the already attached
evidence; this worker did not rerun those identities. This worker triggered and
collected all three new tooling and lane workflows recorded above.

The public matrix runs format, vet, build, and the whole tool test package in
one uncached race-enabled process, including unit/refusal tests and
`TestFixtureEndToEnd`. That test launches the built CLI for every tool and repeats
generation, projection, inventory, mutations and consumer preparation on equal
inputs, requiring byte-identical trees and reports. It executes module and
consumer tests, opt-in/tagged fixture tests, a consumer build and the cmd entry
point. Artifacts include candidate bytes, proxy bytes, reports, inventories,
module graph and sanitized environment identities.

Revision 2 adds `TestMutationRejectsInvalidLifecycleEvidence`, which drives both
CLI entry points with terminal-only baseline/mutant streams, conflicting or
duplicate terminals, duplicate runs and unfinished named/other executions.
`TestLifecycleNarrowingMutant` narrows production enforcement to source guards
and requires that named CLI regression to fail (exit 1, named test and package
failure), recording a 1/1 kill ratio. The ordinary suite remains unmasked; only
the deliberate narrowing experiment uses a named regression mask. Real fixture
kills and compile/unrelated-failure refusals remain in the same suite.

| Run | Ubuntu job duration | macOS job duration | Result |
| --- | --- | --- | --- |
| [38015721398](https://github.com/relux-works/swarma-session-host/actions/runs/38015721398), initial snapshot | 40 s | 59 s | Both green |
| [38015843173](https://github.com/relux-works/swarma-session-host/actions/runs/38015843173), strengthened refusal tests | 31 s | 43 s | Both green |
| [38016057110](https://github.com/relux-works/swarma-session-host/actions/runs/38016057110), implementation and workflow verification | 43 s | 62 s | Both green |
| [38019374197](https://github.com/relux-works/swarma-session-host/actions/runs/38019374197), revision 2 regressions and narrowing proof | 42 s | 48 s | Both green |

Durations are measured from the hosted jobs' started/completed timestamps,
including setup and artifact upload. All recorded snapshots passed every
public matrix step. These measurements concern synthetic tooling only; source/consumer lane durations are recorded in the evidence section above.
