# Internal extension seams

There is no external plugin loader, plugin-registration command or installable
plugin interface in this checkout. It has compiled, repository-internal Go
extension seams. Keep new implementations inside this module's `internal/`
import boundary; wiring one requires source changes and its own review.
The current CLI's adapter switch recognizes only `json-fixture`,
`line-fixture` and `codex`. Do not reuse the launcher's closed plugin schema
as a Host registration API. [C19](evidence.md#c19)

## Synthetic adapters

Implement `sessionhost.Adapter`: `Name() string`,
`InputState() InputState`, `Encode(Notice, Selection) ([]byte, error)`,
and `Applied([]byte, Notice, Selection) (bool, error)`. InputState must be a
bounded nonblocking local observation. Applied must correlate the full intent;
incomplete evidence returns false and malformed evidence returns an error.
Use `fixtureadapters.JSON` and `Lines` as actual encoding/ack examples.
Inject the instance in `sessionhost.Config.Adapter`; dynamic fixture evidence
can call `Host.ObserveInput()`. A CLI adapter additionally needs explicit
construction in the existing serve switch; no registry scans a directory.
[C19](evidence.md#c19)

Test a new synthetic implementation alongside
`TestAcknowledgementsBindFullIntent`, `TestPluginsAndHostReplacement`,
`TestInputEligibilityRequiresIdleEvidence` and
`TestSecurityStopCannotAutomaticallyContinue`. Pair valid delivery with wrong
ID/reference/selection, partial/malformed acknowledgement and unknown/replacement
refusal. These fixtures are inspected examples, not executed qualification.
[C08](evidence.md#c08) [C10](evidence.md#c10)

### In-memory construction example

Run the [compiled adapter example](../scripts/adapter-example.go) from the
repository root with local Go 1.25.5, **after** completing the
[current-composition cache prerequisite](usage.md#current-composition-cache-prerequisite).
Although the example performs only in-memory work, its `sessionhost` import
compiles the normal manager core and requires the exact pinned session-manager
module. Keep the prepared task-owned `HOME`, `TMPDIR`, `GOMODCACHE` and `GOCACHE`
paths in the cleared, network-isolated environment described there. A cold cache
must fail rather than fetch dependencies or use a foreign cache:

```sh
GOTOOLCHAIN=local GOWORK=off GOPROXY=off GOSUMDB=off go run ./agents/skills/swarma-session-host/scripts/adapter-example.go
```

It implements the existing `Adapter` seam by embedding the landed line codec,
supplying a local input-state observation and assigning the value to
`Config.Adapter`. It checks a matching acknowledgement beside a different-model
refusal, an incomplete frame and malformed output. Missing idle evidence stays
unknown. Only the matching full intent may return applied: a wrong model cannot
prove this selection was applied, and a frame without LF is incomplete evidence.

This example only constructs configuration and inspects bytes in memory. It
opens no Host, socket or child and needs no credentials, native runtime or model.
The new name is not a CLI adapter: `serve -adapter example-fixture` returns
`unsupported_adapter` before reserving an endpoint because the serve switch has
no such construction branch. Do not add a plugin manifest or treat assignment
as launch admission. Host delivery still needs the separately configured runner,
private state and readiness gates described above. [C19](evidence.md#c19)

## Native protocol drivers

Implement `adapterprotocol.Driver` with `Descriptor`, `Begin`, `Prepare`,
`Observe` and `Snapshot`. These are local state-machine proposals with no
I/O; the Host owns persistence and runner writes. Declare boundary version,
observed release and only qualified capabilities. Separate action keys, input
IDs, native acceptance receipts, turn identity, completion, activity and
owner-only attention. `Validate` enforces shared bounds/correlation; it does
not validate provider semantics or confer launch authority. Unsupported input
modes return `ErrUnsupported` without actions or checkpoint change.
[C19](evidence.md#c19)

Wire `Config.ProtocolName` plus
`ProtocolFactory func([]byte) (adapterprotocol.Driver, uint64, error)`, leaving
`Config.Adapter` nil and supplying an explicit poll interval. The factory
restores metadata without I/O and returns the safe complete-frame replay offset;
it can be called repeatedly for consumption. The Host validates name, snapshot,
release consistency and replay bounds. Opt-in work-reference drivers must also
declare immutable `NoticeMode() string`; no declaration means synthetic only.
The Codex serve branch is the current concrete wiring example.
[C19](evidence.md#c19) [C12](evidence.md#c12)

Use the contract/capability fixtures for version, framing, identity and unsupported
mode neighbors; use Codex driver/checkpoint tests for a concrete ordered parser
and restore model. Add Host-level retained-outbox, output-gap, terminal-reply
and durable-consumption checks for a new driver. Claude/Muse schematic vectors
are shared-boundary examples, not native adapter implementations or release
qualification. [C11](evidence.md#c11) [C14](evidence.md#c14)
[C19](evidence.md#c19)

## Other seams

`sessionhost.Config.Admission` optionally injects `admission.Config`:
a frozen expected snapshot, read-only `Observe(context.Context)` returning
Record/Lease, and `Process(context.Context, int)` returning Process. Callbacks
must honor the caller deadline. Exact immutable bindings and nondecreasing
revisions are checked; loss latches failed/unknown and prevents automatic writes.
Use the admission pair matrix and post-Encode revocation fixture when changing
this seam. It has no real lease mutation methods or production authority.
[C21](evidence.md#c21)

Notification codec functions can be used inside the module, but stream,
journal installation, manager custody and verified enrollment interfaces are
private. Their implementations and direct constructors in tests do not form a
public registration or activation API. To investigate the seam, inspect the
[notification lifecycle](../../../../docs/notification-lifecycle.md) and its
inert tests; do not invent a facade, proof constructor, G07 grant consumer or
control-tool command. [C16](evidence.md#c16) [C18](evidence.md#c18)
