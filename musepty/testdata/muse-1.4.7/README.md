# Muse 1.4.7 capture witnesses

`session.jsonl` and `screen.txt` are sanitized unsubmitted startup captures.
`submitted-tree/2026/10/14/01a136e3-960b-7b53-be21-9cfa1cd0ed9b/`
contains the sanitized submitted Echo session and its two reminder
subagent logs. `screen-after-submitted-turn.txt` is its post-turn still.
The submitted turn was captured with live HOME, a fresh XDG tree,
`--trust-workspace`, `MUSE_NO_AUTO_UPDATE=1`, and the direct muse-bin entry.

`corpus.json` contains only the two sanitized Echo primary logs, in original order.
`screen_rows.json` retains the earlier startup and trust-dialog cases.
`synthetic-meta-shapes.json` contains minimal schema witnesses with invented
content and identifiers for the additional META shapes. Persistent tests never
access private logs.

Record members, types, nesting, order, payload discriminators and schema
versions are preserved. Workspace paths are `/home/fixture/ws`; home-style
screen footers show `~/ws`. Model context, skills, rules and reminder prompts
use short synthetic text with the relevant XML tag skeletons. IDs and causation
references are unchanged. Rules text offsets and digests refer to synthetic
rules; request digests hash the sanitized configured event JSON. Permission
frame digests hash the newline-joined sanitized child record JSON strings;
this is a fixture convention, since the fold validates digest spelling only.

`TestMusePTY147FixturesContainNoPrivateOperatorValues` scans every fixture file
and path. `TestMusePTY147SanitizedSessionLogAdmission` admits both primary logs
through the production fold and preserves the two subagent logs' existing
refusal: they contain the unsupported `run_fatal_error_classified` kind and
lack an opened session. These nested logs are discovery-depth witnesses, not
host-admissible sessions. Corpus equality, synthetic protocol shapes and screen
grammar are covered by the replay tests. Admitting every log would require a
separate production contract for subagent sessions, beyond value sanitization.

All capture/data files have fixed SHA-256 pins in `SHA256SUMS` and
`TestMusePTY147CaptureSHA256`. Run from `tools/board-cli`:

```sh
GOWORK=off go test -count=1 ./internal/sessionmanager/musepty/ -run 'TestMusePTY(147|141|ClosedGrammarCaptureCompleteness)'
```

The submitted still has no escape sequences. The production VT stores no
SGR attributes. The exact composer row `❯ Type @ to search and insert workspace
file paths` is normalized to empty only within the captured composer geometry
and with no nonempty continuation. Identical typed text cannot be distinguished
with this evidence/model; changed wording and nonempty continuations remain typed.

`screen-meta-startup-plain.txt` and `screen-meta-startup-sgr.txt` are the
sanitized plain and `capture-pane -e` META startup stills: 120 columns, an
empty composer, no rules notice, and the four-part model/effort/path/mode
footer. Only the abbreviated path was replaced with `/h/f/ws`; other bytes,
including SGR, are preserved. The SGR still shows the `❯ ` prompt after a
reset in foreground palette index 214, distinct from the adjacent dim
index-242 rules. It contains no placeholder and no typed comparison, so it
proves prompt styling only and does not resolve the identical-typed-hint
ambiguity above. Both stills pass the production screen and host idle
predicates with an idle fixture fold and a deterministic quiet boundary.
The footer admits only `Auto-review`; yolo/full-access labels need captured
evidence before admission.

These are capture replays and scripted-peer delivery tests, not a live manager
attach or a fresh real-provider execution.
