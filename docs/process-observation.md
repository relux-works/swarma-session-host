# Retained process evidence

The selected manager retains private process evidence through its ordinary
execution owner. This implements the ownership and durable-decision portion
of the observation contract. The current wire schema and capability list are
unchanged; these internal types are not an alternative session-manager schema.
Negotiated projection requires the owning schema module's accepted revision.

This keeps the Swarma book's [launch and session-host chapter](https://github.com/relux-works/swarma-docs/blob/b57a9be633c245373b448c3786bcbd49d636fe8a/book/ru/08-zapusk-i-host-sessiy.md):
uncertain starts retain their reservation, and the existing Host dispatcher
remains the single input writer.

The admission predicates remain those in
[M3 §§1–3](m3-hosted-dependencies.md): the owner incarnation, operation,
execution and generation, profile/home/plan binding and revision, effective
UID, running executable, actual harness role and explicit exec handoff.
The ordinary owner supplies these from its admitted record, independently of
the RPC and public status snapshot. It supplies the actual retained runner
object. There is no public proof constructor or request-supplied proof.

The runner captures raw process identity before its reaper begins. On Linux
the original atomic CLONE_PIDFD handle and a pinned genuine proc directory
remain under that reaper's custody. Fresh reads check the real and effective
UIDs, boot UUID, stat field 22 and the opened running image's SHA-256.
An identify-v2 snapshot may describe an owned zombie; live process evidence
refuses it. No process handle is opened by a saved numeric PID.

On Darwin the original unreaped-child authority protects the raw
`pbi_start_tvsec`, `pbi_start_tvusec` and `pbi_ruid` reads. Existing effective
`pbi_uid`, boot and CDHash admission checks remain in force. The raw birth
fields are retained directly alongside the existing private formatted birth;
they are never reconstructed from that formatted value. This path requires
native Darwin qualification; a Linux pass does not qualify it.

The manager persists the started result, raw identity and a digest of the
private admission record before allowing the retained proof to be observed.
Every proof observation rechecks the same runner object, kernel identity and
private admission record. It returns a copy. Missing persistence, lost hold,
changed image/role/handoff/effective UID/binding/revision, ambiguity or lookup
failure omits evidence. A new daemon cannot reconstruct ownership from disk.
This implementation conservatively requires the exact captured revision.

## Durable no-start decisions

Only a serialized decision before ordinary execution can establish no-start:
an effect-free admission refusal, or cancellation that durably wins against
the final pre-execute reservation. The journal stores that decision explicitly.
Historical records without the decision remain unknown.

An existing reservation is an obligation across errors and restarts. A nil
execution, lost spawn or transport reply, failed persistence, incomplete
reconciliation, failed lookup or missing record cannot establish no-start.
The journal rejects converting a reservation to a no-start decision and
rejects reopening an explicit terminal decision. Exact historical lookup
never executes the operation again. Wire outcome negotiation is separate.

Neither process evidence nor no-start acquires, binds, releases or breaks a
lease, opens an input fence, starts/stops/signals a process, reconciles a
journal or changes a generation. There is no descendant-termination proof.
Direct-child exit and reap establish no descendant absence, including on
ordinary-UID Darwin. Core's independent observer and bound-lease gate remain
required before productive input.

## Verification

With the repository's documented private Go environment and Go 1.25.5:

```sh
go test -p 1 -race -count=1 -timeout=5m \
  -run 'TestNoStart|TestHeldProcess|TestHeldLinux' ./internal/sessionhost ./internal/runner
python3 -m unittest discover -s tools/ci -p 'test_module_checks.py' -v
python3 tools/ci/module_checks.py
gofmt -l internal cmd manager tools
```

Capture outputs under task-owned `.temp/`. Linux controls use disposable
owned fixtures and a synthetic ordinary admission oracle. They establish
neither native-provider admission nor Core bind compatibility. The
Darwin-only raw-reader tests require a separately admitted native fixture
run; a cross-build cannot substitute for it.
