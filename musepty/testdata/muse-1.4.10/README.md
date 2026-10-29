# Muse 1.4.10 capture witnesses

Captured on 2026-10-18 from Muse 1.4.10-R6427.1 (aarch64, SHA-256
`11cc1ae887b4975bbbe1de7358ee9260a612444df9ce4dbfffa1dd785826dbcf`, equal to
the release manifest checksum) with the Echo provider, the direct muse-bin
entry, `--trust-workspace`, `MUSE_NO_AUTO_UPDATE=1` and a fresh XDG tree
for isolated capture.

- `session.jsonl` is the startup prefix of a temp-HOME session, through the
  settled session-name allocation. It witnesses the three records 1.4.10
  writes before the open: `runtime.command_intake.received` with
  `command.kind` `session_start` (payload schema 2),
  `runtime.session_start.intake_v3` and `session.started`.
- `resume.jsonl` is the same startup prefix of the
  `TestMusePTYReal141EchoSmoke` session followed by its clean `session.end`
  and resume block (route, start triple, opened with `resume: true`,
  resumed, settled, startup phases).
- `corpus.json` holds the distinct records of both logs, in order.
- `screen.txt` and `screen-after-submitted-turn.txt` are temp-HOME stills
  with the new `Voice input` label on the upper composer rule.
- `screen-rules-warning-startup.txt` is a live-HOME startup still: the
  banner, the soft-wrapped rules warning, the personal-rules notice, the
  composer and the Echo footer.

Sanitization: workspace paths are `/home/fixture/ws` in logs and `/h/f/ws`
in footers; socket, sandbox and session-log paths sit under
`/home/fixture` or `/tmp`. The rules path is replaced by the same-length
`/home/fix/.claude/CLAUDE.md`, so the warning wraps exactly as captured.
No turn records are kept: they carry the model request and input trace.
Two fixture conventions follow from that cut. The permission frame digest
is the captured value (the fold validates its spelling only), and the
resume citation names the last kept record of the prior epoch (sequence 21)
instead of the dropped run terminal. IDs, order, members and schema
versions are otherwise unchanged.

`TestMusePTY1410CaptureSHA256` pins every file in `SHA256SUMS`, and
`TestMusePTY1410FixturesContainNoPrivateOperatorValues` scans them. Run from
`tools/board-cli`:

```sh
GOWORK=off go test -count=1 ./internal/sessionmanager/musepty/ -run 'TestMusePTY1410|Completeness'
```
