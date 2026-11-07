# Bounded local Host dogfood composition

This candidate merges reviewed single-trust-unit profile `40588b48` (on cleanup
`024fb071`) with frozen Linux v2 component `2086e46f`. Both original heads and
trees remain in history. Frozen composition `47077d97` and its existing output
package are preserved. This signed successor implements owner-approved OI-1
selection and the exact revision-3 startup contract; it also explicitly selects
v2 in the tagged inert lifecycle witness. This is a local review candidate.

## First available use: explicitly synthetic

Build the Linux/amd64 standalone witness from the exact reviewed candidate:

```sh
GOMAXPROCS=2 go build -p 2 -tags t4fixture -o /absolute/own/output/host-dogfood-fixture ./cmd/host-dogfood-fixture
GOMAXPROCS=2 /absolute/own/output/host-dogfood-fixture -root /absolute/own/output/fresh-synthetic-root
```

The root must not exist. The executable preserves private child workspaces,
readiness/TERM files and Host journals there. A held root directory and short
`/proc/<self>/fd/<dirfd>/<case>/r.sock` paths keep Unix sockets inside that root
without truncating or relocating evidence. The alias is fixture plumbing, never
an executable-image proof or security boundary. Existing paths are refused.
No provider/model/launcher/payload/credential arguments are accepted. Ordinary
builds exclude the entire executable with the `t4fixture` build constraint.

One standalone OS process owns an actual Host and runner and two sequential
self-executable children. Each child has no descendants and uses only the inert
JSON notice protocol. Each case captures a connected-peer PID/euid observation,
one Linux v2 owned-child reply, one applied synthetic notice, and actual Host
stop/retry/runner-close receipts. The cooperative case registers its TERM handler
before writing readiness, writes `term-received`, and must return `stopped` with
exit 0. The TERM-ignoring case must return `stop_unknown` after bounded forced
reap; retry remains unknown and Close returns `runner_stop_unknown`. Both must
join their owned direct child and remove their socket. An immediate PID absence
observation is audit evidence only, never authority to signal a remembered PID.
`stop_unknown` does not fabricate `wait_unknown`: its actual receipt can report
`exited:true` after forced reap while cooperative completion remains uncertain.

The JSON explicitly states `synthetic:true`, `real_provider:false`, zero model/
provider calls, no Source factory/ACK, no private create admission, no native
admission and no runtime cutover. The runtime profile field is a **description**,
not proof that this direct fixture traversed private create-policy admission.
The existing independent `t4-private-fixture` executable/test exercises the
private profile/policy/create/retained-cleanup path with separately labeled fake
Core, Source, enrollment/ACK and native protocol output. Its local fake ACK is
not an invented production Source contract. These two evidence sets are kept
separate; their coexistence does not close a real integration seam.

## Blocking seams and the shortest real path

| Seam | Concrete missing evidence/change | Owner/input | Effect |
| --- | --- | --- | --- |
| Source origin and installed ACK | Real provider-origin factory, retained scope/lifetime and installed ACK contract; no replacement constructed here | Source owner | Blocks any real session; not this inert witness |
| NRC-1 startup selection | Implemented here: false-default `Config.IdentifyV2` / `--identify-v2`, selected pre-Listen preflight, one absolute 1000ms observation deadline and sole Wait | Contract `c4e9b6c1115d90de68a9dd49ea7ddf584ec7c501532f994035ca5c80f8c32bf0`; OI-1 owner approval and independent technical text acceptance recorded separately | Exact successor implementation review and Source custody/landing remain required |
| Connected peer and child verification | SO_PEERCRED PID/euid plus an owned tuple are insufficient: fresh runner incarnation/boot/scope/image proof, post-decode child observation, credential/image sandwich and final live check remain absent; Linux `ObserveProcess` still refuses | Peer/verifier owner and Source integration; assignment/ETA not supplied | Blocks real identity admission |
| Image/backend qualification | `plain-pathname-v1` hash covers the launcher only; final check/exec is non-atomic. Interpreter, loader and downstream native images remain outside it; no sealed-procfd qualification | Launcher/image owner; assignment/ETA not supplied | Blocks real launcher/native qualification; inert self-child is not a substitute |
| Admitted selection/profile records | Profile report binds no configured admitted agent UID/scope/image records. `serve` does not select the private composition. Provider/model/effort in the witness are explicitly fake opaque data | Launcher/profile integration owner; assignment/ETA not supplied | Blocks installed real route; no environment or Plan JSON selection invented |
| Exact review and source custody | Component review rev5 required CR-1; exact r3 text is technically accepted with OI-1 owner approval supplied; this successor needs independent review; Source applies/signs/lands under its own custody | Independent reviewer and Source | Blocks landing; producer checks are not review |

After those inputs: integrate in a new bounded candidate, rerun actual launcher/
identity/order/lifecycle qualification, obtain exact-head independent review
and Source signed landing, then seek separately authorized noncritical first
real session. There is no production authorization. Production O/S/C principals,
UID-switch helper and per-agent isolation remain outside this single trusted OS
principal. Additional-host/fleet, snapshot/clone detection and full image qualification
are not claimed. Preserve fresh-only/no-replay cleanup behavior.

`docs/runner-identify-v2.md` documents the selected/default implementation and
remaining admission limits. The old component documentation and frozen artifact
bytes remain in the preserved predecessor. The external contract and reviews
are pinned in the successor review package; Source contract custody is separate.
