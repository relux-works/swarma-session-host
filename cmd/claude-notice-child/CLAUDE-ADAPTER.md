# Claude notice child

## Built boundary

`claude-notice-child` is a one-shot envelope shim behind the existing
`json-fixture` adapter. The host and runner are unchanged. A notice contains
an ID and a prompt-file reference. The shim runs one native Claude process
per notice, keeps native stdout and stderr in workspace files, and emits
only an `applied` envelope on successful completion. Each notice gets a new
UUID session and `--no-session-persistence`; sessions do not continue across
notices. This command does not register `serve -adapter claude`.

The fixture seam (`sessionhost.Adapter`) correlates notice envelopes. It
has no native turn, approval or usage model. A future continuing native
adapter belongs at `adapterprotocol.Driver`, with checkpoint/outbox/fact
semantics; a native stream-json driver is design-only and is not built here.
Its wire contract requires authorized logged-in captures before implementation.
Do not infer stream-json shapes from one-shot JSON results.

## Admission and configuration

Use an admitted, dedicated workspace, private output directory and native
binary/HOME. The caller owns binary and ancestor custody, supported release
selection, model/effort selection, permission policy, settings and credential
admission. The shim is not an OS sandbox or content-based secret detector.
Any qualifying regular workspace file can be a prompt. References must only
be issued for admitted prompt content. No credential or session file should
be placed in that input surface.

| Environment variable | Contract |
| --- | --- |
| `CLAUDE_CHILD_WORKSPACE` | Absolute workspace; defaults to current directory |
| `CLAUDE_CHILD_BIN` | Required absolute native executable; no PATH lookup |
| `CLAUDE_CHILD_MODEL` | Required explicit model |
| `CLAUDE_CHILD_EFFORT` | Empty or `low`, `medium`, `high`, `xhigh`, `max` |
| `CLAUDE_CHILD_PERMISSION_MODE` | Default `manual`; allowlist: `manual`, `dontAsk`, `acceptEdits`, `plan`, `auto` |
| `CLAUDE_CHILD_ALLOWED_TOOLS` | Optional single argv value; policy must be admitted |
| `CLAUDE_CHILD_EXPECTED_VERSION` | Nonempty full trimmed `--version` output for exact comparison; empty disables comparison |
| `CLAUDE_CHILD_OUTDIR` | Absolute directory within the canonical workspace; defaults to `output` |
| `CLAUDE_CHILD_TIMEOUT_SECONDS` | Integer 10–3600; default 600 |

Productive use requires a nonempty expected-version value. A mismatch
refuses with `claude_version_mismatch` before notice processing. Version
checking does not attest immutable executable/interpreter custody. Updating
the admitted version requires repeating CLI flag, valid/error result,
permission and stop compatibility checks. No hardcoded vendor version pin
or successful model invocation is claimed by this shim.

`bypassPermissions`, flag spellings and unlisted permission-mode strings
are refused as `unsupported_permission_mode`. There is no extra-argv or
shell-command passthrough. Prompts go on stdin, never into argv. The tests
exercise every supported mode, bypass spellings, fixed argv structure and
`--model=value` and `--allowedTools=value` binding of flag-like values. These prove the config/argv
boundary only; native interpretation of option values still belongs to
release qualification.

Effective native permission containment is unqualified. Native environment,
HOME/workspace settings, hooks, plugins and prompt-mediated tool choices
are not isolated by this slice. Effective-permission qualification is a
prerequisite for productive use of this shim as well as a future driver.
An authorized capture must cover each allowed mode, the native settings and
environment surfaces emitted by the launch pipeline, tool denials and
forbidden widening attempts, with working positive controls. Fake binaries
cannot establish native enforcement. No-login refusal tests cannot establish
it either. Until that evidence exists, only inert fixture and refusal runs
are qualified; configuration validation is not a wider permission guarantee.

## Files, parsing and completion

1. Validate notice IDs before any path-derived side effect, including failure
   receipts. IDs are 1–64 ASCII letters/digits/dot/underscore/hyphen with no
   `..` substring. Invalid IDs return exactly `invalid_notice_id` and create
   no ID-derived file. The receipt writer independently applies the same rule.
2. Resolve the prompt within the workspace, then open it through an anchored
   `os.Root`. Inspect and read that opened file, never reopen a checked path
   with unrestricted `ReadFile`. A replaced ancestor cannot redirect the
   actual open outside the root. Relative and absolute internal symlinks are
   supported via canonical resolution followed by a rooted open; external
   links refuse. The actual opened object must be a nonempty regular file
   of at most 1 MiB. Reads are capped at 1 MiB plus one byte, including growth.
3. Open captures through a rooted output directory with exclusive creation
   and mode 0600. Output directories cannot escape the workspace, including
   through symlinks. Existing artifacts are preserved. Native stdout is
   bounded at 16 MiB; overflow refuses `result_truncated`. Stderr remains a
   private native capture and is not retained in host state.
4. Require one JSON object and exit zero. Reject duplicate JSON keys at every
   depth, including extension objects. Require present non-null, exact-typed
   `type`, `session_id`, `is_error` and `permission_denials`. Require
   `type:"result"`, the exact preallocated session, `is_error:false`, and an
   empty denials array. Unknown unique vendor fields remain compatible.
   A `subtype:"success"` value is not success evidence: an error boolean
   always refuses. Missing or invalid evidence never invents `is_error:false`
   in a receipt. Trailing JSON and nonzero exits never acknowledge.
5. Sync captures, write and sync a private temporary receipt, then publish it
   by an exclusive hard link and sync the output directory. The final path
   is never opened for truncation; an existing file, symlink, FIFO or
   directory causes refusal without replacing it or following it. Temporary
   names are removed. A duplicate notice cannot overwrite an earlier receipt.
6. Recheck cancellation and parsed success after receipt I/O, before emitting
   `applied`. Cancellation during receipt durability never acknowledges.
   The completion boundary is the final context check before writing the
   envelope; a stop arriving after that boundary cannot retract bytes already
   sent. A durable receipt alone is not host acknowledgment. A cancellation
   racing publication or a failed stdout write may leave a receipt without
   an acknowledgment, so recovery must inspect host state and never replay
   an uncertain notice automatically.

Failure classes include `result_required_evidence`, `result_unparseable`,
`result_not_single_object`, `result_session_mismatch`, `claude_result_error`,
`permission_denied`, `claude_run_failed`, `claude_run_interrupted`,
`output_exists_or_unavailable`, `receipt_write_failed`, and bounded-input
or output-directory errors. Failure receipts are best-effort and exclusive;
receipt failure is not permission to overwrite existing evidence.

## Retention and stop

| Surface | Retained information |
| --- | --- |
| Host journal | Notice/reference, selection and lifecycle; no native result body |
| Runner stdout | Small correlated envelope only |
| Workspace captures | Native stdout JSON and stderr; private payload custody required |
| Receipt | Notice/reference, selection, native session, error/subtype, exit, usage, denial count, timestamps and stdout hash/count; no result text |

Queued/unknown/applied remain host states. Dispatch uncertainty stays unknown
and is never automatic replay. This shim cannot answer native approvals;
permission denials fail the notice. A future driver must represent approval
as authority-required attention without retaining question payloads.

TERM/INT ownership is established before the first native invocation. The
initial `--version` probe uses the same process-group start, TERM/KILL cleanup
and direct-child reaping as a notice run, with a ten-second deadline derived
from the shutdown context. A stop during that probe cancels and joins it
before the shim exits 3 with `claude_run_interrupted` on stderr and no applied
envelope. TERM/INT after initialization, including while waiting for stdin, has
the same exit/error result. Normal stdin EOF exits 0. Version mismatch and
unavailable/failed probes still refuse before notices with exit 2; full exact
version comparison is retained. An interrupted probe produces no notice receipt
because no notice has started.

Every native invocation starts in its own process group. Cancellation sends TERM,
waits one second, sends KILL if necessary and reaps the direct native child.
On cooperative leader exit it also kills remaining members of that group.
Normal leader completion also kills any remaining same-group descendants,
including descendants that redirected their output and did not delay `Wait`.
The direct child is reaped by `Wait`; orphaned descendants are reaped by the OS.
Startup cannot succeed until the completed probe's group is observed absent.
Failure to observe group disappearance refuses normal completion
(`claude_group_cleanup_failed`, mapped to `claude_version_unavailable` for a
probe). This also prevents acknowledgment of a notice with failed group cleanup.
Group disappearance observation is bounded by one further second. This fits
the supported runner five-second cooperative budget (`-io-timeout 5s
-stop-signal term`). Shorter runner budgets are not qualified. The
ignore-TERM timing test and integration witness must pass with this setup.
Runner stop proves reaping of its direct shim child. The shim tests cover
its direct native child and an inert same-group descendant; detached native
tool/MCP descendants and real Claude descendant cleanup remain unqualified.
No survival claim is inferred from a cooperative fake alone.

## Validation and first productive run prerequisites

Run from the module root with the declared Go toolchain and cgo toolchain:

```sh
go test ./...
go test -race -count=1 ./cmd/claude-notice-child ./internal/fixtureadapters ./internal/runner
```

The command tests include malformed/missing/null/duplicate evidence, valid
vendor extensions, nonzero exits, denial/session mismatch, invalid IDs,
output/symlink/FIFO refusal, preserved duplicate evidence, bounded prompt
reads, ancestor replacement, receipt-sync cancellation, permission argv
compatibility and native ignore-TERM cleanup. Fixture tests prove their
exercised filesystem/process boundaries, not logged-in native enforcement.
Completed-probe tests establish a live same-group descendant before permitting
the leader to exit, redirect its output, verify leader reaping, and require
group absence on EOF, version mismatch, TERM and INT, plus descendant absence
before the next notice invocation. The ordinary successful
version-plus-notice test remains the neighbouring positive control.

Before any productive run, all of these must be satisfied:

- B1: credential owner admits the Claude login host and account. Never
  work around missing login or borrow another authenticated session.
- B2: the runtime owner admits the dedicated workspace/HOME and executable
  custody, target OS and runner stop budget.
- B3: the task owner supplies admitted prompt references and retention policy.
- B4: the permission policy owner supplies the effective-permission matrix
  above; this applies to the shim, not only a future native driver.
- B5: the source owner obtains independent acceptance of the exact candidate,
  required checks and normal repository delivery/adoption.
- B6: the run owner selects model, effort, timeout and exact native release.

End-to-end qualification uses F (fake success across host replacement with
retained runner/shim), R (real binary without login refuses and never applies),
S (stop during a running native fixture), plus an ignore-TERM S witness under
the supplied runner timing. If no admitted binary exists, R is unavailable;
a synthetic refusal control must be labeled synthetic, never real-binary
success. Linux and Darwin evidence are separate. No logged-in capture,
permission guarantee, productive readiness date or complete native driver
is implied by green fixture lanes.

Future driver prerequisites G1/G2 are an authorized logged-in capture host
and exact-version valid/error/approval/stop wire captures. Frame bounds,
release migration, native driver registration and effective permissions
require independent qualification. Effort estimates and dates depend on
those inputs; none is a completion guarantee.
