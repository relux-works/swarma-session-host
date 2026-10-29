# Muse PTY runtime

The runtime composes a managed Muse launch, verifies its seal and frozen build
before execution, and owns the terminal, screen grammar and native log fold.
Goal text is supplied by the consumer through `DeliverGoalText`; canonical
board revisions and goal ledgers are outside this package. Existing launch and
snapshot contract strings are preserved for protocol compatibility.

## Tools

Use the repository Go toolchain to build with `go build ./musepty` and inspect
with `go vet ./musepty`. Run bounded unit cases with
`go test -count=1 ./musepty -run '<test-name>'`. Recorded fixtures live in
`testdata/`; the terminal corpus runs against local scripted peers. Native
provider smoke tests require explicit opt-in and are separate qualification.
Mutation tests remain behind the `mutants` tag and their existing opt-in flags.
Store local command logs under `.temp/`.

Canonical conversion tests belong to the consuming application. The pinned
submitted-text corpus here is data used to prove terminal-byte parity, not an
implementation of a board or its goal authority.
