# Session-host debug attach contract — 0.2.1

2026-11-09. **Version 0.2.1 — an editorial publication rendering of accepted 0.2.0 (sha256 `759106a74dec9772eeadaedcd4e70cf1ccb369cadd52a7858e3ab59267c7eace`); no normative change.** This full replacement incorporates the supplied independent review requirements and the launcher owner's written agreement on the launcher surface, including its capability scope and eight-code registry limit. Acceptance of that surface is not publication, implementation, native qualification, integrated acceptance or real-lane admission. Section 2 alone describes inspected implementation; MUST/REFUSE requirements elsewhere describe behavior to build. A changed contract requires a new independent verdict bound to its exact hash.

## 1. Scope and settled policy

Use the existing Curator session-manager `attach` operation, immutable spawn admission `--debug-attach`, a host-owned viewer of the existing app-server stream, and one serialized input owner. The host remains the sole app-server controller. The viewer receives neither raw native stdin nor the runner controller connection. Do not launch a second native resume/picker or substitute a tmux conversation for this contract.

The supplied accepted analysis and subsequent owner decisions settle the following. They are requirements, not questions to reopen:

- Cooperative same-user debugging is admitted now. Shared-UID isolation is explicitly absent; later OS isolation is separate work and is not a prerequisite for this debugging contract.
- The manager can answer qualified native interactions within the subordinate's assigned task/workspace. Credentials, sudo, payment, outward acts and destruction outside that task require the owner. Unsupported interaction classes refuse.
- Admission is explicit. For a selected manager/runtime advertising `debug-attach/1`, an attached launch/resume without `--debug-attach` refuses before spawn. This predicate is keyed on the advertised capability, not a manager name. `--detach` means “not now” and does not disable later attachment to an admitted execution.
- Output custody is an 8 MiB/15 minute volatile tail. Input/necessary interaction custody is a private 0600 journal under the execution state root, capped at 4 MiB and purged at retirement. There is no durable full output transcript in v1. The analysis's earlier suggested 24-hour post-retirement retention is superseded.
- Alarms follow the settled alarm routing: worker → manager; upper manager → operator backup sink; the operator backup sink is backup. Unacknowledged terminal results have a 15 minute deadline. Busy without progress has a 30 minute deadline, then escalation. Earlier analysis timing suggestions are superseded.
- No real lane before unit G. The first real lane is the operator-selected Mac Codex lane. A manual direct-run demonstration is labeled **demo only** and cannot substitute for hosted admission or G.

The launcher carries the admission flag and closed refusal classifications, never the stream, messages, questions, commands, results, grants, writer state or input bodies. Grants, writer arbitration, retention and viewer behavior belong to session-host. This document specifies the external agreement surface without choosing curator-run's internal implementation.

## 2. Exact sources, freshness and present behavior

`SH/` means the session-host source default at **`52e82319fe943d08b3d9a97c59309e4b7990514f`**. The comparison base is `0eeb3d7769e85cf09b848ddb103f7e4e3ba41315`; source evidence was read with `git show <revision>:<path>`. `CL/` means `relux-works/curator-agent-launcher` at `6793333bcd415c1a574986dff03135adc69d33d3`. These aliases bind every file:line citation below to the exact source revision, not to a moving default branch.

Both `git -C <repo> fetch origin` commands were repeated for the accepted 0.2.0 source inspection, exit 0, at the **2026-11-09T18:05:04Z** inspection and final verification at **2026-11-09T18:21:39Z**. Fetched main remains at the exact SH/CL revisions above; there are zero commits since the initial contract inspection on either main, including the cited files. Remote HEAD is `refs/heads/main` for both. Same-file/source and PR checks still show no implementation or superseding debug-attach contract. The source snapshots and recorded `pr-92` revision were preserved. Original source commit clocks and the PR merge time below are separate facts.

The recorded public PR API inspection returned all 34 host PRs (none open) and 96 launcher PRs in single pages. Host [PR #34](https://github.com/relux-works/swarma-session-host/pull/34) **landed at 2026-11-09T17:34:06Z**, with head/default revision `52e82319fe943d08b3d9a97c59309e4b7990514f`. Its 21-file patch changes the adapter pin/comment from 0.164.0 to **0.167.0**, adds a synthetic app-server fixture, documents PATH and adjusts qualification fixtures. On the two adapter logic files, the only changes are `ObservedRelease` and the release comment; it adds no attach mechanism. This contract's current citations and pin are rebased to that landed tree. A changed pin and synthetic fixture are not a native approval/attach qualification result. The older qualification prose at `SH/UNRESOLVED_QUESTIONS.md:39` still names 0.164.0; this contract does not re-label that older evidence as a native 0.167.0 pass.

Launcher [draft PR #92](https://github.com/relux-works/curator-agent-launcher/pull/92) remains at `802895a2fbbc3e49a7bb01ac59674343ec1f720d`, verified against remote PR head and recorded `pr-92` revision. It adds optional `notification` to launch/resume and readable session records, extends the refusal capability vocabulary, and moves session-record/session titles to 1.2.0. Its 55-file patch is an overlapping schema dependency, not debug attach. Draft [PR #95](https://github.com/relux-works/curator-agent-launcher/pull/95) remains open at `a3ee91391176dfa55a4d512c44c71d59572186c1`. The launcher owner sequences attach implementation after #95 and on or after #92's final shape. No competing debug-attach PR was present in the checked PR inventory. **The proceed condition is satisfied; the requested behavior is neither done nor superseded.**

| Evidence | Implemented fact and implication |
| --- | --- |
| E01 | `SH/internal/sessionhost/selected_observation.go:24` advertises launch/resume/status/cancel and optionally notification, without attach. `SH/cmd/swarma-session-host/main.go:30` exposes that selected-manager RPC route. General attach prose in `SH/spec/session-host.md:30` is a target, not implemented admission. |
| E02 | `SH/internal/sessionhost/server.go:89` dispatches status/enqueue/hold/release/stop, not attach, text or native answers. `SH/internal/adapterprotocol/contract.go:79` has reference input, not arbitrary text/answer fields. `SH/internal/sessionhost/host.go:43` explicitly limits Notice to a reference. |
| E03 | `SH/internal/codexadapter/protocol.go:298` ignores other non-turn notifications outside work-item handling. `SH/internal/codexadapter/protocol.go:340` retains item correlation/type while discarding commands/output/patches; six item types are admitted. `SH/internal/codexadapter/work_reference_test.go:240` contains an inert payload-absence witness. |
| E04 | `SH/internal/codexadapter/protocol.go:152` stops semantic consumption in an authority wait; `SH/internal/codexadapter/protocol.go:169` reduces any server method+id to authority-required. `SH/internal/codexadapter/driver.go:154` creates unclassified owner-only attention, not the original request ID/body. `SH/UNRESOLVED_QUESTIONS.md:44` says notification, question retrieval and approval grants are unimplemented. |
| E05 | `SH/internal/codexadapter/protocol.go:118` admits only idle text submission and bounds text to 4096 bytes. `SH/internal/runner/runner.go:572` serializes writes, reserves `write_unknown` before stdin, and at `:588` separately caps the complete encoded write at 4096 bytes. The complete-frame limit is the binding input limit. |
| E06 | `SH/internal/sessionhost/host.go:428` sequences/persists notice enqueue under the host mutex. `SH/internal/sessionhost/protocol.go:439` checks idle/open/no unknown/pending before persisting intent and preparation. `SH/internal/sessionhost/protocol.go:140` persists checkpoint/outbox/facts before effects. These are the integration points, not an existing writer lease. |
| E07 | `SH/internal/sessionhost/protocol.go:400` commits reduced observation/checkpoint before runner consume. `SH/internal/runner/runner.go:648` clears consumed bytes. `SH/internal/codexadapter/checkpoint.go:34` excludes payload/partial output from metadata checkpoints. A later status viewer cannot reconstruct discarded content. |
| E08 | `SH/internal/sessionhost/protocol.go:243` fences refused observations to unknown. `SH/internal/sessionhost/protocol.go:366` stops polling after protocol failure. `SH/internal/sessionhost/host.go:471` refuses release of an unknown fence. `SH/internal/codexadapter/stream.go:65` closes on gaps/bounds. Attach cannot reopen these fences. |
| E09 | `SH/internal/sessionhost/host.go:633` persists stop intent before the runner call. `SH/internal/sessionhost/host.go:656` releases the host controller without stopping the child. `SH/UNRESOLVED_QUESTIONS.md:3` limits stop proof to the original direct child, not all wrapper descendants. |
| E10 | `SH/internal/localipc/transport.go:62` requires a private parent and creates a 0600 socket. `SH/internal/sessionhost/server.go:74` checks same UID; `:30` explicitly denies same-UID agent isolation. `SH/internal/localipc/identity_darwin.go:10` obtains UID/PID from the connected descriptor, not a manager/owner role. `SH/docs/single-trust-unit.md:8` documents the shared trust limit. |
| E11 | `SH/internal/sessionhost/store.go:21` requires a 0700 state root and 0600 lock/state files without following final symlinks. `SH/internal/sessionhost/store.go:98` caps the state snapshot at the 64 KiB local frame bound. `SH/internal/localipc/transport.go:16` defines that bound. The new body journal must be separate. |
| E12 | `SH/internal/codexadapter/driver.go:39` now pins 0.167.0 after landed PR #34; `:51` advertises pipe/next-turn/activity/idle fencing, without qualified steering, interrupt or human terminal. `SH/internal/codexadapter/driver.go:207` renders fixed synthetic/work-reference prompts. `SH/internal/codexadapter/protocol.go:53` and `:131` bind work-reference mode to separately admitted yolo/approvalPolicy never. Debug admission cannot alter this permission choice. |
| E13 | `SH/internal/sessionhost/selected_manager.go:37` distinguishes ordinary session generation from host epoch and requires live resume to retain the execution object. `SH/UNRESOLVED_QUESTIONS.md:21` records outstanding process-incarnation/home-lease adoption. |
| E14 | `CL/schemas/session-manager/attach-request.schema.json:6` closes attach to `{session, expected_generation}`. `CL/internal/sessionmanager/client/transport_unix.go:289` validates terminal attach, uses fd0–2 for terminal, fd3 for request and fd4 for typed status. `CL/FRONT-DOOR.md:888` distinguishes attach's existing `session_generation_stale` from JSON stop's generation mismatch; `:918` requires an exact persisted native target. |
| E15 | `CL/schemas/session-manager/launch.schema.json:318` and `CL/schemas/session-manager/resume.schema.json:228` have detach and closed objects but no debug admission. `CL/schemas/session-manager/README.md:3` describes published module/version ownership. `CL/internal/sessionmanager/client/capabilities.go:100` requires attach for an initial attached run. `CL/cmd/curator-run/delegated.go:124` starts before attaching unless detach, so pre-spawn missing-flag refusal requires a launcher change. |
| E16 | `CL/internal/hosted/status.go:22` fixes the six-member fd4 refusal envelope, version 1.0.0. `:78` fixes code/message/detail/exit rows, `:132` includes existing generation-stale. `:231` refuses unknown codes, extra keys or nonliteral messages. `CL/internal/sessionmanager/client/transport_unix.go:383` remembers the last fd4 refusal for terminal exit; fd4 is not a live interaction bus. JSON RPC refusals are separately validated at `CL/internal/sessionmanager/client/protocol.go:193`. |
| E17 | `CL/internal/sessionmanager/client/reconcile_unix.go:179` retains `bind_pending` and refuses `process_incarnation_unavailable` after a leased start. Attach cannot fake or bypass bind. `SH/spec/session-host.md:247` forbids holding the state lock across other-service calls; `:255` separates bounded diagnostics from mandatory replay evidence and forbids automatic unknown-approval replay. These are architecture requirements, not claims that this contract is implemented. |
| E18 | `SH/internal/codexadapter/protocol.go:334` returns completed turns to idle; `SH/internal/sessionhost/protocol.go:205` records terminal facts/outcome. `SH/internal/sessionhost/protocol.go:495` polls output and then prepares another queued notice in the same dispatch cycle at `:505`, using the gate at `:439`. No terminal-disposition gate is present; the durable fact and eligibility change in C15 are new requirements. |
| E19 | `CL/internal/sessionmanager/client/transport_unix.go:325` marks status descriptors close-on-exec, then explicitly passes request/status as `ExtraFiles` at `:346` to the attach manager. `SH/internal/runner/runner.go:221` separately gives the native child its stdin/stdout/stderr. Intentional manager inheritance is distinct from forbidden subordinate inheritance; R4 controls inspect actual inert-child access rather than inferring secrecy from these source declarations. |

Existing test source was read as evidence of present intent. No product tests or live native sessions were run in this document-only assignment.

## 3. Admission, schema and negotiation

### C01 — immutable admission and the capability-scoped pre-spawn rule

CLI spelling is `--debug-attach`. Launch/resume member **`debug_attach`** is an optional JSON boolean. Absence means false; null/string/integer/duplicate-key values refuse through closed validation. The normal launcher emits only `"debug_attach":true`, omitting the member for absent/false. Explicit false is valid for a direct revised-schema caller but is not serialized by the normal launcher.

Define enrollment from the selected manager's authoritative describe result: **`debug-attach/1` is advertised for the selected manager/runtime contract**. This capability, not literal `session-host` identity, path, notice mode or client preference, scopes the missing-flag rule. Within an enrolled route, **the launcher owns user-facing preflight**: `detach == false` and effective requested debug bit false returns `attach_not_enabled` before `StartLeased`, manager launch/resume, lease acquisition, execution reservation, native spawn or attach. The manager independently enforces that same enrolled-route predicate during admission before effects, including for old/direct callers. A caller cannot evade the manager's rule by omitting or falsifying its own negotiation claim. Both sides use V01 with zero native starts/writes. Read-only discovery is permitted before refusal; post-spawn attach rejection alone is insufficient.

For a route **without** `debug-attach/1`, a no-flag launch/resume keeps its existing capability checks and attached/detached semantics; this revision does not add V01 there or enroll a manager by name. An explicit debug request or present revised member requires the feature/version and refuses V02 before effects when unavailable, never stripping it and retrying. A manager named something other than session-host that advertises the capability must enforce the rule; a nonadvertising legacy manager named session-host must not acquire the new rule merely from its name. A terminal attach to a disabled execution of an enrolled manager remains V01. Do not fall back to native input/tmux to evade admission.

### C02 — optional revision without legacy byte/digest drift

The launch/resume schema adds optional `debug_attach` without changing old required-member lists or object grammars. Its readable session-record/session projection carries only the optional boolean. **The schema/module owner fixes version numbers at build time against #92's final shape**, rather than assigning draft version numbers here. This implements the accepted A02 decision; publication/alignment and actual consumer adoption are still required.

The accepted describe capability **`debug-attach/1`**, alongside existing `attach/1` and the selected runtime's attach operation, commits the manager/runtime to this admission semantics, compatible optional schema revision, eight-code public registry extension and host viewer contract. It is support, never per-session permission. Advertise it only after implementation readiness. The host-internal per-execution advertisement `{debug_attach:true, contract:"host-debug-attach/1"}` binds the immutable execution record within manager/host custody, not a new launcher payload channel.

For an explicit debug request or a present revised member, the launcher requires all those feature/operation/version predicates before sending it. A missing feature is V02 before effects; never strip the member and retry. An old closed manager validator rejects the unknown member (its old `launch_plan_invalid` is acceptable there). No-flag requests to nonenrolled routes keep old behavior. Conformance must prove refusal of the new member by old schemas and cannot rely on permissive JSON unmarshalling. Unknown fields/codes remain refused at their own closed boundaries.

**No-flag compatibility invariant:** previously emitted launch/resume bytes, canonical bytes, request/operation digests and existing profile/plan digests remain identical when the member is absent. Do not inject false, schema-version members or digest salts into legacy requests. Preserve disabled execution serialization/checkpoints as well. Enabled requests include the boolean in the existing canonical request digest path; profile/fragment and notice digests do not change. Explicit false from a direct caller has its supplied bytes and must not be rewritten before validating the recorded operation digest. Pin byte/digest goldens to #92's final shape, with/without its optional notification. A no-flag attached plan retains its encoding but is newly refused only on a `debug-attach/1` enrolled route; other routes retain prior attached checks and behavior.

### C03 — detach and notice/work compatibility

| Request on a `debug-attach/1` enrolled route | Initial behavior | Later attach |
| --- | --- | --- |
| true, detach true | Spawn admitted execution; create bounded debug custody from spawn; no viewer now. | Current manager/owner grant can attach. |
| true, detach false | Spawn only after negotiation/admission; finish authoritative process binding; invoke existing attach on the returned exact session/generation. | Detach/reattach allowed under current grants. |
| absent/false, detach true | Preserve legacy request bytes/digests and ordinary execution behavior. No new debug endpoint, tail or body journal. | `attach_not_enabled`, including for the owner. |
| absent/false, detach false | Launcher refuses `attach_not_enabled` before spawn; manager independently rejects a direct bypass. | No execution is created by the refused operation. |
| true but capability/schema unsupported | `attach_capability_unsupported` before spawn. | No fallback to tmux, native stdin or an unfenced viewer. |

Outside enrollment, no-flag attached/detached requests retain legacy semantics; explicit debug requests without the feature refuse V02. These controls are independent of a literal manager name.

Synthetic notice and work-reference modes retain their existing fixed prompt, grammar, permission policy, receipts and notice digests. Debug admission is orthogonal to the notice mode. Text uses a new typed host input kind; never overload `Notice.Reference`, `native_tail`, labels, environment or notification content. Neither the flag nor manager grant changes yolo, approvalPolicy, sandbox, workspace or task authority (E02, E12).

### C04 — record, host replacement and resume

Record admission before spawn in the authoritative selected-manager execution record and in host execution metadata. Mirror it through the negotiated readable session-record/session projection only as the optional boolean. No grants, tokens, writer state, tail cursors or input bodies enter launcher records. Missing legacy evidence is false; never infer admission from the existence of output, endpoint, a label or a capability.

The bit is immutable for one execution/runner incarnation. A host replacement reloads the same bit and plan binding; it does not convert the replacement into a new launch or reset the bit. Old writer leases and native-answer authority are invalid after host epoch replacement. An enabled execution may have a new empty volatile tail and explicit history gap; this does not rewrite admission.

For a **live execution resume**, the request's absent=false bit must match the recorded bit: after the mandatory C01 missing-flag check, false→true and true→false both refuse proposed `attach_admission_conflict` with no new spawn, write or bit mutation. An absent/false attached resume has already refused `attach_not_enabled` at that earlier check; the bit-conflict class covers mismatches that reach live-resume comparison (including detached resume). Thus resuming a debug-enabled execution explicitly repeats `--debug-attach`, including detached resume. This exact bit-matching rule stands accepted in A03; it is not a silent inherit rule. A host restart using the retained execution record needs no new CLI flag. If native resume creates a **new execution**, it requires fresh explicit admission for that new execution; it cannot upgrade the old one. The manager owns distinguishing these cases using its existing authoritative lifecycle.

### C05 — attach target and fencing

Keep the attach request exactly `{session, expected_generation}` and fd0–4 transport; do not add grants or stream fields to it. Resolve an exact persisted thread, never “latest”. After verifying caller admission, the manager checks current session generation, completed process/home-lease bind, selected manager registration/incarnation and current host/runner/native-thread binding. A wrong ordinary generation remains existing `session_generation_stale`, exit 1, details `{}`. Missing bind is `attach_binding_unavailable`; a positively mismatched current binding is `attach_binding_conflict`. Preserve any earlier authoritative leased-start failure, including existing process-incarnation refusal; do not rewrite it as successful attachment.

Host operations additionally bind session generation, execution ID, runner instance/incarnation, host epoch, native thread, expected turn/interaction and writer revision. These are distinct fences. Revalidate for every authority-creating operation and after asynchronous calls; they are not aliases for `expected_generation`. The manager obtains the private host attachment binding/grant through its registered local route. The launcher transports only the existing target request. No per-session advertisement means `attach_not_enabled` if false; true admission with an unavailable implementation/bind refuses the capability/binding class, without enabling a fallback.

## 4. Agreed launcher boundary and separate host-private refusals

### C06 — closed public registry and host-private results

The supplied launcher-owner verdict accepts the launcher surface with the C01 capability scope and **only V01–V08** added to the launcher fd4/JSON registry. **V09–V22 remain host-private**; new V23 is also host-private. None of V09–V23 is registered, emitted or normalized as a valid launcher fd4/JSON refusal, even if a private failure ends the viewer. This is the binding registry partition, replacing the broader 0.1.0 proposal. It changes no installed receiver until published/adopted by its owner.

Retain exactly six fd4 members and constants: `schema = urn:relux:task-board:session-launch-status`, `schema_version = 1.0.0`, `type = refusal`, `code`, `message`, `details`. Add only these eight agreed rows; change no existing message/exit/detail vocabulary or status type. Every new row has exactly `{}` details, no optional reason/path/body/identifier/digest. Version/module numbers are fixed at build time against #92's final shape. Unknown codes, private-only codes, extra keys, changed literal messages and unsupported envelopes continue to normalize to existing `session_host_protocol_error`, exit 6, empty details (E16).

L = launcher local preflight/fixed diagnostic, which does not emit fd4 to itself. M = selected manager. M/H = host detects a closed class and the manager maps that class without content. The selected manager emits fd4 only for a public refusal of terminal attach or a public refusal terminating it. The separate JSON refusal/outcome/local-diagnostic validators recognize the same eight public additions where semantically applicable. There is no launcher-facing interaction/status expansion for private inputs.

| Public code | Exit | Literal fd4 message | Exact trigger | Detecting side | Vector |
| --- | --- | --- | --- | --- | --- |
| `attach_not_enabled` | 1 | Attach is not enabled. | Initial attached request on a debug-attach/1 enrolled route has absent/false admission; or attach targets an execution with absent/false recorded admission. | L before spawn; M admission before spawn; M/H at later attach | V01 |
| `attach_capability_unsupported` | 6 | Debug attach is unsupported. | Requested/present revised member lacks negotiated debug-attach/1, attach/1, runtime attach or supported contract revision. | L before spawn; M before execution if a direct request cannot be honored | V02 |
| `attach_admission_conflict` | 1 | Debug attach admission conflicts. | A live resume bit differs from that execution's immutable bit after the earlier attached-without-flag check has passed. | M before resume effects | V03 |
| `attach_unauthorized` | 16 | Attach is unauthorized. | No current manager/owner view grant, wrong grant scope/actor/peer, or invalid initial grant. | M/H before payload disclosure | V04 |
| `attach_grant_revoked` | 16 | Attach grant is unavailable. | An already attached view grant is revoked or expires. | M/H; close attachment and invalidate unissued inputs | V05 |
| `attach_binding_unavailable` | 1 | Attach binding is unavailable. | Required bind/incarnation/liveness evidence is missing, unconfirmed or inaccessible. | M/H before attachment/effect | V06 |
| `attach_binding_conflict` | 1 | Attach binding conflicts. | Evidence positively names a different manager/host/runner incarnation, execution, endpoint or native thread. | M/H before attachment/effect | V07 |
| `attach_in_use` | 1 | Attach is already in use. | A valid attachment occupies the sole v1 attachment slot. | M/H at slot allocation | V08 |

Host-private operation results use the closed four-member envelope `{contract:"host-debug-attach/1", ok:false, code:<known private code>, details:{}}`. The local renderer obtains only the fixed display text below from the code; arbitrary error text is not accepted. No OS exit or launcher registry entry is assigned to these private classes. Their detector/emitter is the host; the registered manager can receive them only over the private viewer/control route. Appendix B supplies the exact private vectors.

| Private code | Fixed local display text | Exact trigger | Vector |
| --- | --- | --- | --- |
| `input_writer_required` | Input writer is required. | Required operation rights are present but there is no current claimed writer lease. Missing rights use V23; a formerly valid stale revision uses V11. | V09 |
| `input_busy` | Input target is busy. | An ordinary follow-up targets a current busy turn; no queue/steer is qualified. | V10 |
| `input_observation_stale` | Input observation is stale. | Otherwise current input uses an old host epoch, writer revision or observed conversation revision/turn. | V11 |
| `input_fenced` | Input is fenced. | Unknown/security fence, parser/reader loss, unresolved prior effect, recovery or another nonwriter safety hold forbids the input. Writer's own notice pause alone is not this refusal. | V12 |
| `input_stopped` | Input target is stopped. | Stop intent already won serialization, or the execution is retired/stopped. | V13 |
| `input_invalid` | Input is invalid. | Unknown/extra input fields, malformed/empty/NUL text, missing target, wrong kind shape or invalid native answer option. | V14 |
| `input_conflict` | Input identity conflicts. | Same authenticated actor/input ID is reused with changed immutable intent or payload. | V15 |
| `input_capacity_exceeded` | Input capacity exceeded. | Complete native frame exceeds 4096 bytes, a required protocol bound is exceeded, or journal plus reservations would exceed the 4 MiB limit/control reserve. | V16 |
| `input_storage_unavailable` | Input storage is unavailable. | Required private journal creation, privacy, write, sync or intent commit cannot be established. | V17 |
| `input_effect_unknown` | Input effect is unknown. | Issued input has ambiguous pipe/native outcome and cannot be proven applied/absent by retained evidence. | V18 |
| `input_operation_unsupported` | Input operation is unsupported. | Caller requests unqualified busy queueing, steering, interrupt or an unadvertised input operation. | V19 |
| `interaction_unsupported` | Interaction is unsupported. | A server request method/version/shape has no positively qualified response implementation. | V20 |
| `interaction_stale` | Interaction is stale. | Native interaction is resolved, expired, invalidated by fence/restart, or its native request ID/options digest/target no longer matches. | V21 |
| `interaction_owner_required` | Interaction requires the owner. | Supported decision exceeds manager task/workspace delegation, or is credentials/sudo/payment/outward act/destruction outside the task. | V22 |
| `input_right_required` | Input right is required. | A current registered caller lacks any C07 right/administrative authority required for the private operation; refuse before writer/disposition state mutation, answer CAS, stop intent or native effect. | V23 |

A private failure stays in its private receipt/result, whether the viewer continues or closes. In particular, neither recoverable input refusals nor final private error results enter fd4/JSON launcher diagnostics. If a private failure closes the viewer, ordinary terminal exit reports only its exit outcome; exact private classification remains in host custody. A separately established public attach/grant/transport failure may use its own applicable public code, never a fabricated mapping of a private code. Today's terminal client remembers the last fd4 refusal (E16); emitting a transient private code there would both violate custody and poison detach success. Successful ordinary detach emits no refusal, and exit 0 proves detach only.

Appendix A contains V00 (unchanged generation control) and V01–V08 as exact public vectors; Appendix B alone contains V09–V23. Public code V01 is scoped by C01 at launch/resume. Except V18's historical unknown effect, a rejected private attempt creates zero new native effect; no rejection disproves an earlier issued effect or authorizes replay.

Authenticate sufficient caller/grant authority to name the target first: public attach denial V04 or active grant loss V05 reveals no private target details. Authorized attach order is malformed request → target generation → immutable admission/capability → current binding → slot. C01's enrolled-route preflight precedes live-target lookup. For private operations after fresh identity/grant validation, check operation rights (V23) **before** any mutation or effect and before disclosing a private receipt not covered by view. Then exact authorized historical retry/conflict → stop → lease → stale observation → safety fence → kind eligibility → capacity/storage → immutable effect reservation. An exact resolved retry is receipt lookup, not a new effect, and can remain readable after stop under view authority.

Old receivers still refuse the new public codes rather than silently ignore them until updated. Old no-field detached requests and old known refusal vectors retain bytes/digests; no-flag attached compatibility is scoped by C01. Never hide a private class under `policy_refused`, free-form stderr or a new fd4 type. Curator-run owns only the public eight-code publication/client alignment; session-host owns the private vocabulary and its tests.

## 5. Grants, writer ownership and audit

### C07 — cooperative grants and rights per private operation

Only the recorded accountable manager and owner may receive v1 attachment grants. Same UID, socket knowledge, caller labels or spawn admission are not role authority. Daemon composition registers manager/owner authority through its admitted route. The owner may issue/revoke manager delegation; the registered manager may issue a grant to its verified own endpoint within that delegation, never mint owner authority. Owner takeover explicitly revokes the old slot/grant before claiming anew.

Host-private grants contain an opaque random capability of at least 256 bits, opaque grant ID, registered actor/role, session/generation, execution/current incarnation binding, independent allowlisted rights **`view`, `writer_control`, `text`, `native_response`, `stop`**, allowed interaction classes/task-scope reference, revision and expiry. `writer_control` controls conversation ownership and automation; it grants no text, native answer or stop permission. Deliver grants only to the intended registered manager/owner via a private descriptor/local route. Never place them in argv, child environment, ordinary record/status, workspace or task-created temporary artifacts. Host timestamps/bindings are authoritative; kernel UID/PID/process incarnation supplements registration rather than turning a caller label into a role.

The table gives the minimum rights for a valid, current attached actor. All target/epoch/scope/lease/native preconditions still apply. The same actor, session and fresh binding must be held constant when testing rights. An established private operation missing a required right returns **host-private V23 `input_right_required`**, fixed display `Input right is required.`, details `{}`, before mutation/effect. Initial terminal attach without `view` uses public V04. A grant that is not authentic/current fails the attachment boundary as V04/V05 instead; it is not a rights experiment.

| Private operation | Minimum rights / authority | Additional predicate and denied-effect rule |
| --- | --- | --- |
| Attach/view tail, state, private receipt | `view` | Fresh registered manager/owner binding. No writer or native effect. |
| `ack_terminal` | `view` | Exact terminal fact/revision. Acknowledges receipt only; never clears its delivery block or resumes automation. |
| Claim writer | `view` + `writer_control` | Check rights before acquiring a lease, pausing notices or changing writer state. No right → V23 with no pause/claim. |
| Renew or yield own writer lease | `view` + `writer_control` | Current own lease/revision. Yield only releases the caller's lease into held; it does not resume notices. |
| `resume_automation`, `yield_and_resume`, `detach_with_resume` | `view` + `writer_control` | Known eligible state, all terminal facts explicitly permitting automation, no outstanding native/effect/safety/stop fence. Right denial changes neither writer nor terminal state. |
| `disposition_terminal` (`keep_held` or `permit_automatic`) | `view` + `writer_control` | CAS the named fact's current revision under C15; no implicit disposition of other facts. Tail acknowledgement or a text/answer/stop right is insufficient. |
| `follow_up` | `view` + `writer_control` + `text` | Current claimed writer lease, idle/known target. A pending terminal-result block alone permits this explicit reply; it still blocks automatic notices. |
| `answer_interaction` | `view` + `writer_control` + `native_response` | Current writer lease, qualified pending request/options and allowed task/class scope; `text` is neither necessary nor sufficient. Rights precede answer CAS. |
| Stop while attached | `view` + `stop` | Current stop target and task authority; no writer-control, text, native-response right or writer lease is required. Stop remains available when busy/held/ordinary input capacity is full. Rights precede stop intent. A separate admitted manager stop route likewise does not borrow writer authority. |
| Plain detach / close own view | No additional control right | Always allow transport/resource cleanup of the caller's own attachment. If it owned the writer, revoke its lease into held; never resume notices or stop the child. |
| Issue/revoke grants or owner takeover | Recorded owner, or registered issuer explicitly delegated that administrative action | Never inferred from any attachment right, including `writer_control`. An authenticated caller without that delegation receives private V23 before grant/slot mutation. Revocation by the rightful authority stays reachable independent of writer state. |

For paired controls, `{view,writer_control,text}` may claim and submit ordinary text but cannot answer a native request or stop. Adding only `native_response` permits the qualified answer; adding only `stop` permits stop. `{view}` can inspect/ack/detach but cannot claim, yield-and-resume, resume or disposition. Adding only `writer_control` permits claim and eligible control operations, without creating text/answer/stop rights. These are capability checks, not denials based on actor name or different target state.

Check current grants on connect, each read, claim/renew/control and every effect admission. Revocation increments grant revision, closes its view, invalidates leases/cursors and cancels not-yet-issued intents atomically with the gate. Expiry uses host monotonic deadlines anchored to UTC metadata. Validate late results without reviving authority. Host replacement invalidates old active grant/lease bindings; reacquisition needs fresh registration/binding. A faulted execution may remain viewable as stale, never writable through view alone.

This remains cooperative access control under one shared UID. Modes, PID checks and bearer capabilities cannot exclude malicious unrestricted same-UID peers, process/file replacement or native-owned transcripts. Do not claim OS isolation or add it as a new debugging prerequisite. Preserve 0700 parents, 0600 endpoints/journal and non-following/object checks (E10–E11). Treat new grant/journal/controller/manager-delivery descriptors as noninheritable by default; only an explicitly allowlisted intended recipient gets a descriptor. Intentional launcher→manager fd3/fd4 inheritance (E19) is not permission to pass those or private descriptors on to the subordinate. C13's real-sink synthetic controls must prove that distinction, including reopen/replacement and error cleanup.

### C08 — one attachment, one serialized writer, independent terminal block

V1 permits one attachment total, including view-only attach. Second manager/owner attachment returns V08, without stealing a slot, adding a reader/writer or implicitly revoking the first. The first view opens read-only. C07 `writer_control` is required before a claim changes any state; view-only claim/resume attempts return private V23 without pausing automation.

The **terminal-result block is independent of the writer state**. C15 commits that block when a verified terminal result is observed, even while writer state is `automatic` and no viewer has claimed. No automatic notice reservation is eligible with any blocking terminal fact. Acknowledgement, tail eviction, viewer detach or lease timeout cannot clear it. This is a new host transition, not just an alarm record.

| Writer state | Allowed transitions/effects |
| --- | --- |
| `automatic` | Deliver a notice only if C15's full predicate, including no blocking terminal facts, passes. An authorized claim atomically pauses future notice delivery and enters `claimed`; bounded notices may still enqueue. Read-only view does not itself claim or pause. |
| `claimed` | Current lease plus C07 operation rights permits eligible text/answers. An already issued notice may finish. Renewal retains the state; ordinary yield goes to `held`. Explicit yield-and-resume can enter `automatic` only after rights, disposition and all other eligibility checks. |
| `held` | No automatic notice starts. An authorized current claimant can claim a new revision; explicit control may resume only when idle/known and every required terminal fact permits it, with no native/effect/security/stop fence. |
| `stopped` | No new input. Authorized view/receipt reconciliation remains possible; a writer/control transition cannot reopen it. |

Proposed operational constants remain a 60 second writer lease and renewal interval no greater than 20 seconds using host monotonic time. Renewal checks current rights/binding, never output activity. For an attachment that owned the writer, timeout, revocation, loss or plain detach fences the old revision into `held` unless stopped; it cannot auto-resume, cancel an issued input or kill the child. A read-only attachment that never claimed leaves an automatic writer state automatic on disconnect, **but every terminal fact still blocks delivery**. A plain yield may leave the view attached; default detach leaves an existing writer hold. C07 separately authorizes yield/resume/detach-with-resume. Draft bytes remain viewer-local and are discarded on exit.

Host replacement retains the immutable bit **and every terminal fact's ack/disposition/block**, begins held until recovery, invalidates old grants/writer leases/answer authority and exposes a new epoch/gap. Recovery cannot interpret missing/ambiguous terminal metadata as permission to deliver: hold unknown, alarm and reconcile. An issued input stays recorded for reconciliation rather than replay. Stop authority remains independent of these writer/terminal holds.

### C09 — journal every accepted input with attribution

For debug-enabled executions, every accepted ordinary follow-up, native answer, notice delivery, host protocol control and stop/control intent records authenticated sender/role (host identity for automatic notices), host UTC time, monotonic sequence/time, input ID/kind, immutable target and payload digest, grant/writer revisions, eligibility observation and subsequent result stages. For manager text/answers, retain the necessary body privately. A self-reported actor/time is never authoritative. Do not copy these bodies into the 64 KiB host snapshot, public status, alarms or fd4.

Rejected attempts are not accepted input: return the exact class with zero new native effect. Record bounded content-free rejection metadata in reserved space when available; never promise unbounded per-attempt logs or retain attacker bodies to satisfy auditing. If a required input intent cannot be journaled, it is not accepted and is not written. All input that reaches native stdin must have its sender/time durable beforehand. Grant/stop revocation uses reserved control capacity, rather than deleting unresolved input evidence.

## 6. Viewer, custody and export

### C10 — show structured payloads before today's discard

Add a bounded host-side copy at the ordered complete-frame observation point **before** E03/E04's payload reduction and E07's consumption. Keep one canonical reader; the viewer is a subscriber and cannot advance runner consumption or alter native receipts. The copy is independently bounded and cannot block canonical processing. An enabled host begins capture at spawn even when detached.

Render admitted message text, exposed plan/reasoning items, tool names/arguments/results/output and patches, host input/status/receipts, and qualified native requests with their actual method, question and options. Only show data exposed by the pinned native protocol; never promise hidden reasoning or inaccessible stderr (normal child stderr is discarded at `SH/internal/runner/runner.go:202`). Preserve event boundaries, turn/thread association and order, escape terminal control sequences, and make gap/fault/staleness conspicuous.

A terminal agent message suggesting consent/refusal is **`text_question_candidate`** or **`text_refusal_candidate`**, a nonauthoritative presentation hint; every verified terminal result creates C15's durable delivery block without relying on that hint or a writer claim. Capture and that admission transition both precede any following notice reservation. The initial notification is content-free; the question body remains only in C11's bounded tail. Replying is an ordinary new turn. A native method+id request is **`protocol_interaction`**, which needs precise request correlation and a typed answer. Do not convert text “yes” into protocol consent, label every native request an approval, or let quoted instructions/tool output mint authority. Unknown methods/classes appear as inert diagnostics with `interaction_unsupported`; malformed/unknown authoritative stream observations retain the current closed parser/fence behavior. A debug copy cannot qualify a new event type for work.

### C11 — volatile output tail

Limit retained encoded debug event bytes, including their envelopes, to **8,388,608 bytes** and **900 seconds from host observation**, evicting at either limit. No disk spool/full transcript. Enforce TTL even during silence via timer and again before serving reads. Use bounded event count/metadata as well, charged within that budget; do not retain unbounded pointers, queues or per-view copies outside it. One stream event remains subject to the qualified native frame limit (currently 64 KiB, `SH/internal/codexadapter/protocol.go:15`). Oversized input frames do not relax parser bounds.

The private event envelope uses contract `host-debug-attach/1` and fixed kind plus host epoch, sequence, observation time, execution/thread/turn correlation and qualified payload. `tail_gap` reports unavailable sequence range/frontier and cause (`age`, `capacity`, `host_replaced`, `observer_closed`), without inventing bytes. A reconnect receives only the surviving tail and a current state frontier. A slow viewer loses debug history, not canonical receipt progress. No buffering outside the bound to “catch it up”. Host exit loses the volatile tail; host replacement reports that gap honestly. Tail eviction is not permission to evict required input/approval evidence. C20 sends an initial content-free terminal notification on fact commit, not first at 900 seconds. A read before eviction can show the question; at/after its 900-second expiry or capacity eviction it returns an honest `tail_gap`/body-unavailable result. The terminal fact and its delivery block survive that loss, but the host cannot promise to recover the original words. Do not extend retention or write a transcript to satisfy the alarm.

### C12 — private 4 MiB input/interaction journal and retirement

Proposed pathname: `<execution-state-root>/debug-attach/input.journal`, outside the subordinate workspace, in a 0700 subdirectory; file and any temporary/rotation/commit file are 0600, no symlink traversal or inherited child descriptor. Creation/reopen/replacement and every error path must retain that noninheritance, as demonstrated by C13's actual inert-child descriptor tests. Execution-state-root is the already admitted per-execution state root, not an arbitrary CLI path. Validate retained file object/ownership/privacy on reopen. This is a proposed new artifact, not a claim that E11 creates it today.

The hard **4,194,304 byte** per-execution budget includes all journal bodies, request/options needed to reconcile native answers, attribution/receipts, control records and all live temporary/compaction copies. It is a total custody cap, not a 4 MiB per-message limit. Retain only the necessary bounded interaction body as reconciliation state; no tool/output transcript is written there. Reserve 65,536 bytes within the cap for bounded stop/revoke/capacity and terminal bookkeeping. Additionally reserve the worst-case future receipt space for each accepted input before accepting it. Ordinary input is refused before combined used+reserved bytes exceed the remaining cap. Refuse when required interaction evidence cannot fit, leaving it unanswered and alarming; never silently drop evidence or answer first.

Commit private intent/body with durable attribution and correlate it with host outbox state before acknowledgement/issue. For new text/answer kinds, the outbox references an immutable private journal frame entry instead of copying body bytes into the old snapshot; existing disabled notice/control serialization stays unchanged. Crash recovery across the two records must conservatively hold on incomplete transactions, without introducing body bytes into the old snapshot. The input budget cannot be bypassed using temporary files, terminal history, multiple grants or rotation. No automatic compaction may remove unresolved evidence. Resolved body compaction, if later needed, must preserve receipt/conflict obligations and fit the same total cap; it is not needed to admit v1. Reserve cannot guarantee writes on a broken filesystem; storage failure is reported and new effects remain held.

**Retirement purges the journal, necessary pending interaction bodies, temporary copies, debug grant material and volatile tail.** Detach or stop intent alone is not retirement. Before declaring retirement complete, resolve/disposition outstanding effects under existing lifecycle authority, retain only content-free receipt/tombstone metadata required for reconciliation, close readers/writers and unlink/clear the private artifacts, sync the parent and verify they are inaccessible through the host. An explicitly abandoned unknown effect stays marked unknown in metadata; purge must not rewrite it as “did not happen”. Purge failure leaves retirement incomplete with a retry/alarm; there is no automatic 24-hour exception or durable full-output archive. Host crash leaves the private journal for the owning execution's recovery/retirement path, not for arbitrary startup deletion. Unlink is logical purge, not a claim of physical erasure from swap/backups or native-owned session files.

### C13 — export, redaction and custody at real sinks

V1 has no raw transcript export command. An explicit manager/owner diagnostic summary uses only allowlisted contract/build revisions, fixed classes, relative durations, bound counters, gaps and disposition/check verdicts. Exclude raw text/commands/arguments/output/patches/request options, credentials, private identifiers/paths, grant material and payload-derived digests. Hashing low-entropy data is not redaction. Tail access grants no permission to save it elsewhere. Native-owned files, terminal history and OS storage have separate custody; host retirement does not claim to purge them.

Use **disposable synthetic grant and private-descriptor canaries** to test the actual sinks, not just renderer output or source close-on-exec flags. In an inert subordinate child, inspect its own actual argv/environment and inherited descriptor access. Inventory all inherited descriptors within that fixture and attempt safe read/typed probes of synthetic grant-delivery, journal and runner-controller sentinel endpoints. The subordinate must have neither grant bytes nor a usable grant/journal/controller/manager-delivery descriptor; a changed descriptor number or an otherwise private pathname is not a pass. Normal native stdin/stdout/stderr and any explicitly documented legitimate pipeline descriptors remain allowed. Test the positive allowlist of intended descriptors, not a blanket rejection of fd numbers above 2.

Inspect ordinary session/operation/readable record projections, status, stderr/fd4, alarms, summaries, checkpoints, the fixture workspace and every task-created temporary/error artifact for those synthetic grant/body/descriptor canaries. Do not scan another task's files or any live credential/session/auth path. The private authority may retain a grant only at its designed private custody locus; the native child's workspace/temp, ordinary records and all public/diagnostic outputs are not that locus.

Pair the subordinate noninheritance control with **successful intended manager descriptor delivery and attachment**. The terminal manager may intentionally inherit request/status fd3/fd4 (E19) and receive an explicitly routed private grant descriptor; its subordinate may not. Separately hand a harmless sentinel descriptor to a dedicated detector-control child and prove the fixture can detect access, so an empty/ineffective descriptor probe cannot falsely pass secrecy. Only synthetic data is used, with bounded fixture-owned captures.

Repeat through grant delivery, close/reopen, host/manager replacement, journal reopen, partial setup failure, rejected attachment and error cleanup. The intended manager can still obtain a fresh valid grant and attach; old/error descriptors and tokens are revoked/closed, cannot become usable on later child exec and do not leak into temporary artifacts. No credential provisioning, OS isolation or live process inspection is required by these planned controls.

### C14 — observation failure

Private viewer status uses `live`, `stale`, `closed`; writer state uses C08's four values. Interaction state uses `pending`, `answer_reserved`, `effect_unknown`, `resolved`, `expired`, `invalidated`, `unsupported`. These proposed host-private values add **no** session-manager lifecycle state or fd4 type. Report the last verified sequence/frontier on failure; view can remain diagnostic while writes remain closed. Parser failure currently stops polling (E08): v1 freezes the available tail and reports stale, without promising a continued raw drain. A slow/dead viewer is a tail gap, not a parser failure. Preserve this distinction in tests and alarms.

## 7. Input ordering, ordinary follow-up and native interaction

### C15 — atomic terminal facts, disposition and the single admission order

Serialize **terminal-result observation, acknowledgement, disposition/recovery, writer claim/renew/yield/revoke/resume, notice reservation, typed input, native answer CAS and stop** at the host's final admission authority (E06, E18). Bounded notice enqueue remains allowed while delivery is held. Preserve existing notice sequence/digests; use a separate host fact/effect sequence for these interactions.

For every verified terminal turn in a debug-enabled execution with an outstanding assignment, durably create a content-free **terminal-result fact** in the same admitted transition that records its terminal outcome and makes idle visible. It binds session generation/execution/native thread/turn, terminal outcome and observation sequence/time, a stable result ID/revision, acknowledgement state, disposition and **`delivery_permission = blocked`**. Record the initial `terminal_observed` notification intent in the same commit. No text classifier, active viewer or writer claim is required. The body stays only in the volatile tail. If the fact/notification intent cannot be durably committed, do not publish eligible idle: hold unknown and report the storage/observation failure. Terminal-fact metadata and its future transition space are bounded/reserved under C12; a cap cannot authorize omission of a blocking fact.

The exact automatic-delivery predicate is the conjunction of all preexisting known/idle/open/no pending-or-unknown effect/no stop-or-security/recovery conditions, **writer state `automatic`**, and **no retained terminal-result fact with `delivery_permission != permitted` for this execution**. Check it and reserve the next notice under the same admission order. Thus the existing poll→prepare dispatch cycle (E18) cannot observe idle and reserve the next queued notice without seeing the newly committed block. This predicate applies to automatic notices, not a correctly authorized ordinary manager reply. Debug-disabled executions retain their old bytes and behavior.

`ack_terminal(result_id, expected_revision)` with `view` records receipt, not disposition or permission. It never clears a block. `disposition_terminal` requires C07 `writer_control`, current target/epoch and CAS of each **named** fact revision. `keep_held` records a next action/defer while remaining blocked; `permit_automatic` explicitly records permission for that fact only. It cannot clear a later/different fact, safety/native hold or uncertain effect. A bounded exact list may be dispositioned atomically with expected revisions; a vague “all through now” or latest-turn pointer must not silently include unseen results. Delivery resumes only through an authorized disposition/resume transition for which the whole predicate passes. Record acknowledgement and disposition as separate facts even if an explicit command requests both.

The fact/revision/ack/disposition/block and notification debt survive host replacement. Old writer/grant epochs are invalidated, but their loss does not delete the execution's terminal fact. Recovery with missing/ambiguous fact evidence is held/unknown, never “no pending result”. Tail expiry, a timer, alarm delivery/ack, model completion text, closing a view or a newer turn never grants delivery permission. Every completion of a manager follow-up creates its own applicable terminal fact; a reply does not automatically disposition either result. Terminal metadata does not become a durable transcript.

Ordering controls:

- **Terminal first:** its fact and block commit before the next notice reservation. A queued notice cannot reserve or write, even with writer state automatic and no claim. Acknowledging without disposition leaves exactly that state.
- **Disposition/resume first:** under the same gate verify rights/current fact revisions, record explicit permitted disposition and eligibility, then reserve at most the next intended notice. A new terminal fact observed before reservation blocks it; a stale post-replacement disposition is refused privately as V11. No late result reopens old authority.
- **Notice versus claim:** if notice reservation already won, retain its in-flight turn and let it settle; claim cannot retract it. If claim wins, persist writer pause/revision before notice reservation. Enqueue alone is not delivery authority. Final text/answer submission checks fresh rights/lease/observation/capacity and journals a whole input.
- **Stop/revoke:** if first, invalidate future issue; if an immutable issued/in-flight marker already won, retain and reconcile that one effect rather than claiming byte cancellation or resending it.

A manager with current `{view,writer_control,text}` can claim and send an ordinary idle follow-up **while a terminal fact remains blocked**. The block holds automatic delivery rather than denying the explicit reply. Subsequent reply completion and final disposition/resume are separately observed, attributed and serialized. Unsupported native waits, unknown/security/stop fences and busy state remain ineligible; the terminal block is not a bypass of them.

Never hold the state lock across runner/service I/O (E17). Persist immutable intent/in-flight marker under the gate, release it for calls, then revalidate late results without reopening authority. One host outbox/write sequence prevents byte interleave. Rejections lacking rights happen before any writer pause, terminal disposition, answer CAS or stop marker. Read-only rendering/drafting alone reserves nothing.

### C16 — ordinary follow-up

Private input kind `follow_up` includes client input ID, whole text, expected conversation revision, native thread/last terminal turn, observed event frontier and current attachment/writer binding. Conversation revision changes on input-affecting native/host transitions, not every harmless render-only delta. Host checks C07 `text` plus `writer_control`, a current claimed lease, idle/known tail and no unresolved effect/native interaction, then encodes a new native turn. C15's terminal-result block does not forbid this explicit reply and is not cleared by it. A text-question hint never skips these checks. Preserve multiline/Unicode text and shell metacharacters as data; never shell-evaluate input. Empty/NUL/malformed forms return `input_invalid`. Check the **entire encoded native frame including newline and escaping** against 4096 bytes (E05); a 4096-byte text string can exceed it and must refuse `input_capacity_exceeded`.

No secret busy queue, automatic “yes”, text merge or steering. Explicit unqualified queue/steer/interrupt returns `input_operation_unsupported`. A busy next-turn attempt returns `input_busy`, retaining any draft locally for an explicit later decision. Do not retarget an old draft to a newer turn merely because the session becomes idle.

### C17 — native answer authority and correlation

The adapter owner qualifies a positive method/version/options allowlist on one exact native pin. At present none is implemented (E04); the specimen method `item/commandExecution/requestApproval` in `SH/internal/codexadapter/work_reference_test.go:285` is an inert observation witness, not qualified response support. Unit D must publish exact native wire mappings/ID domains and the required deletion-capable approval binding before advertising `native_response`. This session-host contract defines what that qualification must bind; it does not invent upstream method formats.

Retain the **original typed native request ID**, method/class, canonical request/options digest, task-scope/authority classification, execution/thread/turn, observation/expiry, host epoch and interaction revision. The private `answer_interaction` operation names this exact pending interaction and a permitted option/answer, never free-form fd4 content. Use a host-local opaque interaction ID for handles; it is not a substitute for native correlation. After checking independent `native_response` and `writer_control` rights and the current lease, answer admission compare-and-swaps `pending` to one immutable `answer_reserved`, journals its sender/time/body, then issues at most one qualified native response. Private `resolved` follows only known qualified evidence; unknown effect stays unknown. A second different answer conflicts/stales; an exact retry returns its prior receipt without a second response.

Unknown native methods/versions or unsupported interaction types return `interaction_unsupported`; bad known answer shape/option returns `input_invalid`; wrong/resolved/expired/old-epoch/request-digest binding returns `interaction_stale`. Fence close, retirement and host replacement invalidate unanswered interactions. Do not replay a historical approval or infer denial from absence. A read-only view or text grant cannot answer a native request. A current writer with `{view,writer_control,text}` but no `native_response` returns private V23 before answer CAS or native output; adding only the missing answer right is the paired positive control. Manager scope is checked against task/workspace and explicit allowed class. Supported requests outside that delegation return `interaction_owner_required` and remain unanswered for the owner route. Credentials are handled by the owner's existing credential workflow, never entered into this viewer/journal. Owner involvement does not make an unsupported protocol class supported.

Existing work-reference/yolo uses approvalPolicy never (E12). Debugging does not relax that policy to manufacture a passing approval test. A native approval positive control requires a separately admitted effective policy/route that can actually produce it. If unavailable, report native-answer capability unavailable and G incomplete for that required case; an inert fixture or ordinary text reply cannot satisfy it.

**Minimum required capability at G:** support an actual native **approval gating an authorized deletion of a disposable file inside the assigned task workspace**. This is approval of a deletion-capable action, not merely a tool user-input question, a generic harmless approval or plain-text “may I delete?” message. D identifies the pinned release's precise method/class/ID/options forms that actually gate the deletion (for example, whichever qualified file-change or command action is used), and publishes policy/effect evidence. This contract does not assert an unverified upstream method name or require every deletion mechanism.

The real native positive control must prove: the disposable file exists and the action is waiting for its original native approval; the correctly scoped manager has independent answer rights; one response targets that original typed request/options/turn exactly once; the native action actually continues and the authorized file deletion is observed. A receipt or turn-completion token alone does not prove deletion. Pair it with wrong request ID and wrong option (zero new answer/effect), and an out-of-task/owner-only deletion request with no manager answer and the outside fixture file preserved. Use a qualified denial/no-answer branch where needed to witness the native gating. Also retain a separate **plain-text deletion-consent question → explicit ordinary new-turn reply → observed authorized continuation** case. Neither case substitutes for the other.

G remains incomplete if this deletion approval cannot be produced or correctly answered on the admitted native route/policy. An unrelated supported approval cannot replace it. The existing yolo/approvalPolicy never route is unchanged; any policy used for this control must be separately admitted. No permission change, actual deletion or native launch is authorized by this document-only revision.

### C18 — receipts and retries

Host-private result stages distinguish `accepted`, `issued`, `write_known`, `effect_unknown`, `native_accepted`, `turn_terminal`, `refused` and separate manager disposition. Accepted/transport-written/turn-completed are not task success. Key deduplication by authenticated actor plus input ID and compare the immutable full intent/payload digest, including operation kind/target/expected turn. Exact authorized retry/receipt query returns its original stages; any changed intent under the same key returns `input_conflict`. No new sequence or resend on lost reply. A native answer's known response stage must be qualified separately from ordinary turn completion, because it may have no normal turn/start receipt. An ambiguous pipe/native result returns `input_effect_unknown`, holds writes and preserves evidence for reconciliation. Viewer reconnection is never replay authority.

### C19 — stop, detach and retirement remain different

Detach ends the attachment, releases its slot and invalidates writer ownership; it does not call Stop, cancel issued input or kill the runner. A current viewer/writer without `stop` gets private V23 with zero stop intent or native effect, regardless of its text/answer/writer-control rights. An explicit separately stop-authorized operation records stop intent before the existing runner stop path and invalidates subsequent text/answers. Stop remains reachable while attached, busy, held or at ordinary journal capacity. The viewer shows stop requested/outcome and any remaining identity/cleanup uncertainty. A stopped execution may remain inspectable until retirement under its view grant. Current direct-child-only stop proof remains a limit (E09); do not claim descendants died or full retirement succeeded from detach, EOF or a terminal exit code. Retirement and purge require C12's separate evidence.

## 8. Alarms and acknowledgement

### C20 — deterministic deadlines and routes

C15 atomically commits each terminal fact/block and an initial content-free **`terminal_observed`** notification intent before subsequent automatic delivery. Attempt that notification immediately on the next available notifier dispatch, within 30 seconds in a healthy configured sink; do not wait for the 15-minute escalation. Failed delivery follows C21 retry/backup while retaining debt and original age. Notification intent and ack/disposition survive replacement, without retaining a question body.

Maintain independent host facts: native turn start, last qualified current-turn progress, terminal-turn time/disposition, pending interaction/revision, outstanding input effect, parser/reader frontier and last healthy observation. Do not use process existence, viewer keepalives, model prose or raw bytes as proof of task progress. A qualified native tool/item/turn transition may advance progress; repeated identical events, unrelated thread output and viewer activity may not.

| Alarm | Trigger/deadline and handling |
| --- | --- |
| `interaction_pending` / `interaction_unsupported` | Immediate on a new native interaction. Notify the manager; owner-only classes route for owner action. Never automatically answer. |
| `terminal_observed` | Initial content-free notification at terminal-fact commit, dispatched immediately as above; acknowledgement does not permit automatic delivery. No payload is added to a launcher/status channel. |
| `terminal_unacknowledged` | Every terminal result with an outstanding assignment is tracked, whether or not text heuristics find a question/refusal. If still without manager acknowledgement at **15 minutes**, escalate. Track turn IDs separately so a later result/notice cannot erase the older one. Acknowledgement records receipt of the result; disposition records next action and is separate from task success. An acknowledged result remains delivery-blocking until an explicit authorized `permit_automatic` disposition passes C15; a next-action/defer record alone keeps it blocked. If its text has expired from the bounded tail, report that gap honestly and keep the metadata block; do not invent or persist the original question. |
| `busy_without_progress` | When current native activity remains busy and no qualified progress has occurred for **30 minutes**, emit/escalate. If no progress has ever occurred, age from turn start. Bytes alone never reset the clock. Approval wait is a separate pending-interaction state, not evidence of progress. |
| `input_unresolved` / `input_storage_unavailable` | Immediate on an unknown issued effect or failure to custody a required input. Coalesce retries by incident; do not silently replay. |
| `monitor_stale` / `alarm_delivery_failed` | A separate supervisor detects host/reader sampling or alarm-sink failure, preserves unknown/stale status and retries the existing incident. It cannot certify itself merely by being alive. |

The worker's immediate accountable destination is its manager; the upper manager escalates to the operator backup sink, with the operator backup sink as backup when the manager/upper manager route is unavailable. The upper manager binds actual admitted local sinks/relay and backup before G. Session-host emits only fixed alarm class, opaque local correlation, observation age/gap, delivery/ack/disposition metadata and a private locator; it creates no messaging identity and sends no raw question or output to a coordination channel. Identity-free workers use a manager-local sink plus the upper manager's admitted relay. Delivery, acknowledgement, disposition and resolution are separately recorded; a successful socket write is not an acknowledged alarm.

### C21 — false positives and observer health

Use host monotonic elapsed time; persist relevant UTC anchors/content-free alarm facts for restart and preserve original age on sleep/wake or route recovery. Unknown observation cannot reset deadlines or become idle. Deduplicate by execution/generation/turn or interaction and keep original first-observed time. A manager with the required C07 writer-control authority may record a bounded, explicit defer reason/deadline for a legitimate long tool or acknowledged wait, with the original incident still visible; it cannot silently clear it by keeping the viewer connected. At the defer deadline, re-evaluate/escalate. No alarm auto-stops, auto-consents, changes permissions or declares failure/success.

Proposed implementation health constants for agreement: sample host/reader at most every 30 seconds; two consecutive failed/stale samples emit `monitor_stale` (no claim of native task failure); retry alarm delivery on bounded 5/15/30 second backoff, then at most once per minute with deduplication. These operational sampling/retry constants are proposals, not changes to the settled 15/30 minute thresholds. Tests use a fake clock; actual runtime policy is recorded at qualification. Wake/restart emits one incident with original age, rather than a notification flood. Guaranteed semantic detection of all possible conversational blockage is not claimed; every unacknowledged terminal result receives the deterministic 15-minute safeguard, irrespective of text classification.

## 9. Acceptance matrix — specification for later implementation

These are named planned tests, **not tests run or passed by unit A**. Fixture-only success does not qualify an installed/native route. Tests must include positive controls so “reject everything” is a failure. Every rejection checks unchanged native write/spawn counters and absence of leaked payload where applicable. A result's exact candidate/module/native revisions must be recorded with its actual verdict.

| Clause | Test name | Fixture / controls | Pass condition and limit |
| --- | --- | --- | --- |
| C01 | `AdmissionBeforeSpawn` | Enrolled fake launcher/manager with counted lease/start/attach calls; no-field, false and true, attached/detached. | Missing/false attached returns V01 with zero lease/start/native effects from both launcher and direct-manager bypass; true takes admitted route. No real bind proof. |
| C02 | `LegacyBytesAndDigests` | Final #92 baseline launch/resume goldens, notification absent/present, synthetic/work-reference; old canonical/digest vectors. | No-flag request and disabled-state bytes and all preexisting digests identical; true changes only intended request/record fields and request digest, not profile/notice digests. |
| C02 | `SchemaRevisionAndOldPeers` | New/old modules; absent/false/true, null/string/integer/duplicate/unknown keys; explicit debug vs no-flag legacy request. | Valid new booleans accepted; invalid forms rejected; old validator rejects any new field; explicit debug without feature V02 before spawn; no-flag nonenrolled route stays legacy. |
| C03 | `DetachModeCompatibility` | Both notice modes × true/absent × detach true/false, valid old reference forms and permission policy witnesses. | Exact C03 table; detach delays view without revoking admission; no new debug custody when disabled; old reference/policy bytes unchanged. |
| C04 | `AdmissionResumeImmutability` | False/true and legacy records; live resume matching/mismatched bit with both detach values; same-runner host replacement; new execution resume. | Matching admitted live resume succeeds; a no-flag attached resume is V01 before lookup, other mismatches V03 with no mutation/start; replacement reloads bit without enabling old leases; new execution gets fresh admission. |
| C05 | `AttachGenerationAndIncarnation` | Current generation/bind vs stale generation, missing bind, changed runner/manager/endpoint/thread. | Current exact target opens; stale uses existing generation code; V06/V07 zero disclosure/write; retained process-bind refusal remains a failure, never synthetic success. |
| C05 | `AttachUnadvertised` | Capable manager with per-execution false/legacy record vs true/current record. | False/legacy V01 despite attach/1; positive true/current opens. |
| C06 | `StatusRegistryVectors` | Only Appendix A V00–V08 plus all old public vectors; mutate code/message/details/version/extra field; separate JSON registry. | Eight public additions and old controls have exact class/exit; all unknown/mutated envelopes refuse without reflection; only applicable public RPC forms validate. |
| C06 | `RecoverableRefusalIsNotFd4Event` | Busy/rights/native answer refusal, later successful text/detach; private failure that closes viewer; separately actual grant loss. | Private results including closing failures never enter fd4/JSON registry; normal detach exit 0; public grant loss V05. Exact private outcome stays in host receipt, never fabricated public mapping. |
| C07 | `GrantAllowlistAndRevocation` | Registered manager/owner, view-only actor, foreign cooperative actor, expired/revoked grant, wrong process binding. | Granted manager/owner views; V04 denies unauthorized; revoke closes view with V05, invalidates unissued inputs, future reads/claims denied. Shared-UID adversarial isolation is not claimed. |
| C07/C15 | `RevokeMidAttach` | Barrier before issue vs after issued marker; concurrent revoke and submit. | Revocation first: no new native write; issue first: at most one attributed in-flight effect, reconciled honestly, no replay or second write after revoke. |
| C07/C08 | `SecondAttacherAndWriter` | Sole attachment occupied; second manager/owner attach; view-only text/claim; authorized text caller with no current writer claim; explicit owner takeover. | V08 preserves slot; missing rights V23 before mutation; rights present but never claimed V09; takeover requires explicit authorized revoke and new revision. |
| C07/C08 | `LeaseYieldTimeoutDetach` | Valid control rights, fake 60s/20s lease, yield/expiry/loss/detach, read-only no-claim control and explicit resume. | Writer loss becomes held and preserves terminal blocks; pure view loss does not claim a writer hold; only authorized eligible resume reopens delivery. Rights-denied controls do not mutate. |
| C09 | `JournalSenderTimeBeforeEffect` | Text, answer, automatic notice and stop; barriers at write/sync/host-outbox commit. | Every native input has host sender/time/target intent first; failed durability V17, zero input; caller-forged actor/time ignored. |
| C10 | `PrediscardPayloadView` | Canary messages, tools/args/results/patches, native questions/options on exact fixture schema; debug false control. | Enabled viewer shows available payload safely before discard; disabled path/checkpoints/status remain payload-free; tool event never fabricates completion. |
| C10/C17 | `QuestionVsProtocolApproval` | Plain-text deletion-consent question ending a turn; actual-shaped method+id approval, tool user-input request and unknown method. | Explicit text reply is a new turn; approval requires typed correlated native answer; user-input or unrelated approval cannot prove required deletion approval; unknown V20. |
| C10/C14 | `UnknownEventAndTerminalEscape` | Known valid families paired with new/malformed/oversized items, wrong thread/turn and terminal escape bytes. | Valid families render, unqualified authority does not; unsafe observations retain fences; escape bytes render inertly, never act on terminal. |
| C11 | `TailBytesAgeAndSlowViewer` | Exactly 8 MiB then one byte/event over; 899/900s and silence; stalled viewer and >8 MiB total output. | Retained encoded storage never exceeds cap/age; explicit gaps; canonical checkpoints/consumption continue without viewer backpressure. |
| C11/C14 | `HostLossTailGap` | Replace host after retained output, partial frame and pending interaction. | Old volatile tail is unavailable with explicit gap/stale frontier; immutable bit preserved; old writer/approval invalid; input stays held until reconciliation. |
| C12 | `JournalCapAndReserve` | Exactly full admissible used+reserved budget vs one byte over, pending interaction, and concurrent stop/revoke. | V16 before acceptance/issue at overflow; no eviction of unresolved body; receipt/control reserve works; full encoded frame also capped at 4096. |
| C12 | `JournalPrivacyAndCrash` | 0600 good file, symlink/wrong-mode/object replacement; crash around private-body/outbox commits. | Good private custody works; invalid storage V17; no body in ordinary snapshot; incomplete transaction holds and never replays unknown input. |
| C12/C19 | `RetirementPurgeProof` | Admitted fixture with canary journal, temporary files, ring and open viewer; purge failure then recovery. | Retirement not complete on failure; success closes readers, clears ring/grants and proves no readable debug body/artifact remains under root. Content-free unknown tombstone stays unknown; no physical erasure/native-file claim. |
| C07/C13 | `ExportAndSinkRedaction` | Synthetic grant/body/path/options/digest canaries at actual stderr/fd4/status/alarm/summary/checkpoint/ordinary session-operation-readable projections and fixture workspace/temp sinks. | Only allowlisted summaries; no grant/private-body canary at forbidden sinks or error artifacts; separate intended private grant delivery succeeds; raw transcript export unavailable. |
| C15 | `ClaimVsNoticeBothOrders` | Deterministic barriers at enqueue, delivery reservation, claim and final submit. | Notice-first recorded in-flight and text refuses until eligible; claim-first holds notice and text may submit once; no frame interleave or silent draft retarget. |
| C16 | `TextValidFormsAndEncodedBound` | Multiline Unicode/quotes/backslashes/shell metacharacters at full-frame boundary; empty/NUL and escaped over-bound text. | Legitimate forms round-trip as data; V14 invalid shape, V16 complete-frame overflow; zero shell evaluation. |
| C16 | `BusyAndUnsupportedInput` | Busy next-turn vs idle next-turn; steer/interrupt/queue requests vs advertised supported mode. | V10/V19 with no hidden queue; idle text accepted. |
| C07/C17 | `NativeAnswerBindingAndAuthority` | Qualified deletion approval with original supported ID/options; wrong ID/digest/turn/option, expiry/restart, owner-only/outside target, and text-only current writer. | Exactly one qualified authorized response; V14/V21/V22 for corresponding shape/stale/scope negatives; text-only writer V23 before CAS; no outside deletion or second answer. Unknown V20. |
| C17/C22 | `NativeDeletionApprovalPositiveControl` | Real native approval that gates deletion of a disposable in-task file at exact integrated/native revisions and separately admitted effective policy; wrong ID/option and outside-task branches. | File exists while pending; one original-request-bound authorized answer causes actual native continuation and observed deletion. Wrong ID/option causes zero answer/effect; outside task remains owner-only with no manager deletion. Harmless approval/user-input/fixture-only/never-policy cannot pass; unavailable required route leaves G incomplete. |
| C18 | `RetryConflictAndLostReply` | Drop replies before/after native write, exact actor/ID intent vs changed body/target/kind. | Exact retry returns original receipt, zero second write; changed intent V15; ambiguous effect V18 and closed writes. Native accepted/terminal distinct from task success. |
| C07/C19 | `StopDuringAttach` | Same bound actor with/without stop right, stop-vs-submit/answer barriers, busy/held/full ordinary budget and detach-only control. | Missing stop V23 before stop intent; adding only stop works independently of writer/text/answer rights; stop-first yields V13/no new input; issued effect reconciles; detach leaves child alive. |
| C15/C20 | `TerminalDeadlineAndRouting` | Initial terminal commit, no writer claim, 14:59/15:00 fake clock, consent question/refusal/summary, ack without disposition and manager absent. | Initial content-free notification is attempted immediately (healthy sink within 30s); unacknowledged terminal escalates at 15m; ack never releases notice block; delivery/ack/disposition separate and backup works. |
| C20 | `BusyProgressDeadline` | 29:59/30:00, valid progress vs duplicate/unrelated/raw-byte chatter, legitimate long tool and true hang. | Only qualified progress resets gap; 30m gap escalates without auto-stop/answer; long tool can be explicitly deferred with original incident visible. |
| C21 | `AlarmObserverFailureAndWake` | Two failed 30s samples, notifier failure/backoff, Mac sleep/wake, host restart and defer deadline. | Unknown/stale not idle; original ages survive; one coalesced incident reaches backup; no flood or silent deadline reset. |
| C22 | `IntegratedMacRolloutGate` | G01–G08 checklist below, every applicable other section 9 test at exact revisions, real hosted bind and required deletion/plain-text cases. | Every required lane has actual passing evidence plus exact-candidate cross-review/final acceptance/checks/landing/adoption; unavailable native deletion approval leaves G incomplete. No demo substitutes; first real lane remains held. |
| C01/C02 | `CapabilityScopedMissingFlag` | Hold request/actor/runtime fixed; vary advertised debug-attach/1 and arbitrary manager name. Include enrolled non-session-host name, nonenrolled session-host name, both detach values and direct-manager bypass. | Enrolled missing flag V01 before effects regardless of name; nonenrolled no-flag attached/detached retains prior route/capability behavior; explicit debug unsupported V02. Matching legitimate old-manager requests still execute as before. |
| C06 | `PublicPrivateRegistryPartition` | Updated public receiver/RPC validator, Appendix B private envelopes and forged otherwise-valid fd4/JSON envelopes carrying each V09–V23 code; eight valid public controls. | V01–V08 validate where applicable; every private-only code is refused by both public boundaries, including a closing private error. Private results validate/render only at host boundary. |
| C07/C08/C15 | `GrantRightsByOperation` | Same registered actor/session/fresh binding and eligible per-operation checkpoint; vary only C07 rights/delegation. Exercise attach/read/ack, claim, renew/yield, resume, yield-and-resume, detach-with-resume, disposition, text, answer, stop and administrative grant actions. | Each operation-right denial uses V23 (initial missing view V04; active loss of view V05) before relevant writer/terminal/grant/answer/stop mutation or native effect; minimally sufficient grant succeeds at the same checkpoint. View-only ack/detach works but does not resume or disposition. |
| C07/C17 | `TextWriterCannotAnswerNative` | Current correctly bound writer with view+writer_control+text, qualified pending deletion approval, no native_response; paired grant adds only native_response. | First attempt V23, zero answer CAS/native response/deletion; paired grant adds only the missing right and produces one correlated answer; the operation matrix also admits the minimal no-text answer grant. Text privilege is neither inferred as answer authority nor treated as a stale/busy error. |
| C07/C19 | `StopRightIndependentOfWriter` | Current valid writer with all ordinary rights except stop; paired grant adds only stop; separate view+stop control while busy/held/full budget and without writer lease. | Missing stop V23, unchanged StopRequested/counters/child; explicit stop right succeeds independently of writer_control/text/native_response. Full ordinary input budget does not consume stop reserve. |
| C07/C12/C13 | `GrantAndDescriptorCustodyAtChild` | Synthetic grant bytes and harmless grant/journal/controller/delivery FD sentinels; actual inert child argv/environment/FD probes, ordinary projections, fixture workspace and task temp; intended manager descriptor delivery and sentinel detector control. | Subordinate has no grant bytes or usable private descriptor; normal documented native FDs still work; intended manager receives its grant and attaches. Detector-control child detects its deliberately passed sentinel. No live credential/session/auth files inspected. |
| C07/C12/C13 | `GrantDescriptorReplacementAndCleanup` | Repeat real-sink canary/FD probes across close/reopen, grant/host/manager replacement, journal reopen, partial setup/delivery failure and rejected attach. | Fresh intended manager attach still works; stale/error private FDs and tokens are closed/revoked, absent from subsequent child access and ordinary/workspace/temp artifacts. Cleanup never touches peer/live state. |
| C08/C15/C20 | `TerminalFactBlocksQueuedNotice` | Queued notice N, writer automatic, no claim; current turn ends with consent question. Commit terminal fact/notification intent, ack only, repeat dispatch, then replace host. Fresh grant explicitly dispositions/resumes the exact fact; retry that control. | Zero intervening automatic notice reservations/writes before disposition, including ack-only and replacement; fact/debt/ack survive while tail may not. Explicit current permitted disposition/resume allows exactly one N delivery; stale/duplicate controls cannot create extra writes. |
| C08/C15/C16 | `TerminalReplyWithPendingDisposition` | Same blocked/queued checkpoint; valid view+writer_control+text claims and sends ordinary reply without clearing the first terminal fact. Reply completes; explicitly disposition each fact and yield/resume. | Exactly one new-turn manager reply works while automatic N writes remain zero; reply completion creates a distinct blocking fact. Only explicit correct disposition of all relevant facts plus eligible resume permits N once. |
| C10/C11/C15/C20 | `TerminalNotificationAndTailExpiry` | Terminal question canary, initial notifier dispatch, ack-only, read at 899s then 900s/age eviction, capacity eviction and host replacement variants. | Initial content-free notification is not delayed to 15m; within-retention read shows available text; expired/lost tail reports gap/body unavailable, never restored text. Durable block remains; no transcript/body added to metadata or retention extended. |
| C16/C17/C22 | `PlainTextDeletionQuestionReply` | Separately admitted disposable in-task workflow asks in ordinary text for deletion consent, completes the turn and has no protocol approval request; queued notice present. | C15 blocks automatic notice; explicit manager follow-up is a distinct native turn and authorized deletion/continuation is observed. It neither answers nor counts as the real native-approval positive control. |

For every public refusal row, `StatusRegistryVectors` must pair the exact trigger in section 4 with the nearest legitimate allowed operation from the corresponding row above. Host-private rows use the matching operation tests and `PublicPrivateRegistryPartition`, including independent-rights V23. A decoder-only vector pass proves classification, not the trigger's zero-effect behavior. Source-owner conformance, host fixtures, native qualification and independent final review are distinct evidence lanes.

## 10. Agreement status, publication and build sequence

A contract change returns to affected sides before implementation relies on it. The supplied written launcher verdict accepts A01–A05/A10 with two binding changes: C01 enrollment is keyed on `debug-attach/1`, and the public registry is trimmed to V01–V08. Those decisions are incorporated below; they are not all reopened as pending agreement. This host contract requires exact-hash independent review, publication and implementation readiness. Receipt, source-owner agreement, independent acceptance and installed adoption are distinct facts.

| ID | Accountable side(s) | Exact written decision/evidence needed |
| --- | --- | --- |
| A01 | curator-run + session-host | **Accepted with scope change, incorporated:** optional boolean and same-class launcher/manager pre-spawn enforcement are keyed on advertised `debug-attach/1`, irrespective of manager name; legacy nonenrolled no-flag routes retain behavior. `CapabilityScopedMissingFlag` is the positive/negative alignment control. |
| A02 | curator-run | **Accepted:** final schema/module version numbers are fixed at build time against #92 final shape. Publish optional field/projections, capability and only eight public codes; run unchanged no-flag byte/digest/old-peer alignment and verify actual consumer pins. |
| A03 | curator-run + session-host | **Accepted, retained:** live-resume bit must match, enabled live resume explicitly repeats the flag, same-execution replacement retains admission, new execution needs fresh admission, legacy missing evidence is false. Preserve C01 preflight precedence. |
| A04 | curator-run | **Accepted with trim, incorporated:** only V01–V08 enter launcher fd4/JSON/local registry publication. V09–V22 stay host-private; new rights class V23 is private too. Appendix A/B and `PublicPrivateRegistryPartition` enforce this without changing existing public rows or unknown-code refusal. |
| A05 | session-host + curator-run | **Accepted, retained:** unchanged attach target/fd contract, exact generation/registration/incarnation and completed process/home-lease bind, intended manager descriptor delivery and no launcher stream custody. |
| A06 | session-host | Review/publish private formats and operation-right map including writer_control/V23; terminal observation/disposition/notice predicate, recovery, leases and stop/revoke linearization. Record positive/negative host readiness with no public registry expansion. |
| A07 | session-host | Prove bounded custody/reserves, terminal fact/notification metadata, no disabled artifacts, error/replacement descriptor hygiene and retirement purge. Settled 8 MiB/15 minute tail, 4 MiB input cap and no transcript/24h exception remain unchanged. |
| A08 | adapter/policy owner within session-host | Supply pin-specific methods/IDs/options and independently observed **in-task deletion native approval** evidence plus outside-task/wrong-ID/option negatives and separate plain-text deletion reply. Use the landed 0.167.0 pin or an explicitly reconciled successor; separately admit the effective policy, never change yolo here. |
| A09 | upper manager + session-host | Bind initial terminal notification, admitted manager and upper manager sinks, plus the operator backup sink, acknowledgement distinct from permitted disposition, retries/health and settled 15m/30m thresholds. Prove retention-loss honesty and replacement debt without a new messaging identity. |
| A10 | curator-run | **Accepted, retained:** launcher implementation is separate, after #95 and on/after #92 final shape. Recheck files/defaults at readiness and run final alignment; source publication and installed adoption remain separate. |
| A11 | upper manager + source owners | Supply admitted architecture destination/access and actual review/publication route; publish accepted specification and cite its revision in implementation/docking evidence. This contract alone is not source publication. |
| A12 | upper manager | Route the new exact A candidate for independent review without inheriting 0.1.0 acceptance. At whole-solution readiness obtain cross-review/nonproducer final exact integrated verdict, checks/signed landing/adoption; hold real lanes until G01–G08. |

The supplied source-owner constraints already assign launch/resume/attach/refusal/status schemas, the schema module, client and fd4 receiver to curator-run; this contract does not change that ownership. The supplied written verdict already enumerates A01–A05/A10; this revision implements its two changes and retains the others. Final version allocation/publication, host-side revised readiness and exact-hash independent acceptance require evidence through the existing acceptance route. No acceptance of changed bytes is inferred from the old review. No per-part review chain is introduced.

### C22 — sized build units and the real-lane gate

Sizes below are conditional engineering estimates, not staffing, launch authorization or promises. New reusable harnesses/oracles use Go and Go tests. Each part builds to its own positive/negative readiness against the **published accepted contract revision**, with exact revisions and actual check verdicts. Part elements use `review=none`; accountable evidence acceptance/closure uses the supported existing task flow. At whole-solution readiness, sides check docking both ways, cross-review the other parts and obtain one independent final verdict on the integrated candidate. No part may self-approve.

| Dependency order | Unit / owner | Bounded work and usable readiness | Size |
| --- | --- | --- | --- |
| 0 | A — session-host with curator-run agreement, upper manager publication routing | Complete revised host contract review using the incorporated launcher agreement, then publish the accepted versioned specification. This contract is the specification only; source publication and implementation admission are not inferred. | S: 30–60 min agreement session; publication/review availability separate. |
| 1a | B — session-host | Immutable execution bit; private grant/one-slot viewer skeleton; pre-discard bounded ring; journal attribution/cap/reserve; gate ordering and detach/restart/purge controls. Full readiness includes terminal fact/disposition/replacement ordering with no writer claim, independent rights and real-sink descriptor/custody controls, plus both notice/claim orders and all privacy/capacity negatives. The short inert skeleton is not this full readiness. | M: 1–2 h skeleton plus 3–5 h custody/recovery hardening. |
| 1b → 2 | C — curator-run (launcher/schema/client/status), with session-host terminal adapter | After A and curator-run's #95/#92 sequence: optional schema revision/capability, no-flag byte/digest goldens, pre-spawn rule, record/resume propagation, eight public refusal vectors and existing attach terminal docking; V09–V23 remain private. Scope the rule by capability, not manager name. Joint finish depends B's private interface readiness. Launcher has no stream custody. | M: 2–4 h after source dependencies, publication and pin/adoption separate. |
| 2 | D — session-host adapter/policy owner | After B, on the landed PR #34 pin (or an explicitly reconciled successor): qualify payload families, plain deletion-question follow-up and the required in-task deletion native approval with methods/options/authority/receipts and actual effect on one native release. Declare unsupported cases precisely; no unilateral native permission change. | M: 3–6 h if supported route exists; otherwise source dependency with no invented ETA. |
| 1c → 2 | E — upper manager alarm/relay integration + session-host signal owner | After A, with B/D signals for joint completion: bounded terminal/busy alarms, healthy observer and delivery/ack/disposition/backup route. No second native input writer. | M: 2–4 h; admitted sink availability separate. |
| 1d → 2 | F — curator-run process-incarnation/home-lease owners; upper manager tracks adoption | Complete existing authoritative leased-start/process-bind dependency and current retained manager/runner identity evidence. Reuse its existing source-owner gates; neither attach nor a direct demo bypasses it. | External readiness; no credible ETA assigned here. |
| 3 | G — upper manager qualifies rollout; both source sides dock; independent nonproducer architect accepts | Depends on accepted/published A and ready/adopted B–F including E. Run every applicable section 9 test and the G01–G08 evidence checklist on one exact integrated Mac candidate, cross-check both sides, obtain peer cross-review then final architect verdict, pass required checks and signed landing, verify installed dependency adoption. Record exact schema/host/launcher/native revisions and preserved runtime limits. | M: 2–4 h focused qualification plus actual review/check/landing/adoption time and existing soak requirements. |

**G is the gate the first real lane waits for.** Its exact-candidate evidence must enumerate all applicable section 9 tests, not an ambiguous “C22 matrix”. Every mandatory item below receives an actual PASS, failure or could-not-start/unavailable verdict, with source/schema/native revisions and relevant preserved-state limits. No required R1–R4 control can be waived by calling a weaker positive control supported.

| Gate item | Required evidence / section 9 tests |
| --- | --- |
| G01 — admission and compatibility | `AdmissionBeforeSpawn`, `CapabilityScopedMissingFlag`, `LegacyBytesAndDigests`, `SchemaRevisionAndOldPeers`, `DetachModeCompatibility`, `AdmissionResumeImmutability`, `AttachGenerationAndIncarnation`, `AttachUnadvertised`. Both managers' legitimate capability/name combinations and exact record/process bind must work. |
| G02 — public/private boundary | `StatusRegistryVectors`, `PublicPrivateRegistryPartition`, `RecoverableRefusalIsNotFd4Event`. Exactly eight public additions; all fifteen private-only classes remain rejected by public receivers. |
| G03 — independent rights and control | `GrantAllowlistAndRevocation`, `RevokeMidAttach`, `SecondAttacherAndWriter`, `LeaseYieldTimeoutDetach`, `GrantRightsByOperation`, `TextWriterCannotAnswerNative`, `StopRightIndependentOfWriter`, `StopDuringAttach`. Same-actor/fresh-binding rights-only negative/positive pairs and issued-effect limits are required. |
| G04 — viewer/custody/child sinks | `JournalSenderTimeBeforeEffect`, `PrediscardPayloadView`, `UnknownEventAndTerminalEscape`, `TailBytesAgeAndSlowViewer`, `HostLossTailGap`, `JournalCapAndReserve`, `JournalPrivacyAndCrash`, `RetirementPurgeProof`, `ExportAndSinkRedaction`, `GrantAndDescriptorCustodyAtChild`, `GrantDescriptorReplacementAndCleanup`. Real inert-child argv/env/FD and workspace/temp/record sinks are checked; intended manager delivery still succeeds. |
| G05 — terminal disposition ordering | `ClaimVsNoticeBothOrders`, `TerminalFactBlocksQueuedNotice`, `TerminalReplyWithPendingDisposition`, `TerminalNotificationAndTailExpiry`. Zero queued automatic notice writes before explicit permission, including ack-only/no writer claim/replacement; exactly one permitted delivery afterwards; explicit manager reply and honest expired-tail behavior. |
| G06 — required native deletion and text cases | `TextValidFormsAndEncodedBound`, `BusyAndUnsupportedInput`, `QuestionVsProtocolApproval`, `NativeAnswerBindingAndAuthority`, `NativeDeletionApprovalPositiveControl`, `PlainTextDeletionQuestionReply`, `RetryConflictAndLostReply`. A real original-request-bound deletion approval must produce the actual authorized deletion once; wrong ID/option and outside-task/owner-only requests must not. An unrelated approval, user-input request or text-only reply cannot clear native deletion readiness. |
| G07 — initial notification and deadlines | `TerminalDeadlineAndRouting`, `BusyProgressDeadline`, `AlarmObserverFailureAndWake`. Initial content-free terminal notification, actual route/delivery/ack/disposition/backup, fixed 15m/30m thresholds and sleep/restart evidence. |
| G08 — integrated acceptance/adoption | `IntegratedMacRolloutGate` combines G01–G07 with real hosted process-incarnation/home-lease bind→attach, bidirectional source-owner docking, peer cross-review, independent nonproducer exact integrated verdict, required checks, signed landing and verified installed dependency adoption. No partial/synthetic agreement is native correctness or full descendant-cleanup proof. |

If the required native deletion approval/policy or hosted bind cannot start, G is incomplete; report that lane honestly, retain the hold and route the source dependency. The first real lane after G is the operator-selected Mac Codex lane. A disposable qualification run is distinct from that lane. A manual direct-run demo remains demo only, even when its viewer works; it clears neither F nor G. This document grants no source implementation, native launch, policy change or real deletion permission.

## Appendix A. Exact public fd4/JSON vectors — V00–V08 only

Each fd4 block is one UTF-8 JSON object followed by LF. Its public exit/message/trigger/detecting side is the section 4 table row. Only V01–V08 are additions accepted by the launcher owner; V00 is the unchanged generation control. Publication and adoption are still pending. The current unextended receiver refuses all new public codes until that update.

V00 — existing control, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"session_generation_stale","message":"Session observation is stale.","details":{}}
```

V01 — public `attach_not_enabled`, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_not_enabled","message":"Attach is not enabled.","details":{}}
```

V02 — public `attach_capability_unsupported`, exit 6:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_capability_unsupported","message":"Debug attach is unsupported.","details":{}}
```

V03 — public `attach_admission_conflict`, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_admission_conflict","message":"Debug attach admission conflicts.","details":{}}
```

V04 — public `attach_unauthorized`, exit 16:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_unauthorized","message":"Attach is unauthorized.","details":{}}
```

V05 — public `attach_grant_revoked`, exit 16:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_grant_revoked","message":"Attach grant is unavailable.","details":{}}
```

V06 — public `attach_binding_unavailable`, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_binding_unavailable","message":"Attach binding is unavailable.","details":{}}
```

V07 — public `attach_binding_conflict`, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_binding_conflict","message":"Attach binding conflicts.","details":{}}
```

V08 — public `attach_in_use`, exit 1:

```json
{"schema":"urn:relux:task-board:session-launch-status","schema_version":"1.0.0","type":"refusal","code":"attach_in_use","message":"Attach is already in use.","details":{}}
```

The separate public JSON refusal registry contains these eight code vectors, used only for an operation with the corresponding applicable trigger. It has no message member. Registry recognition is not authorization to fabricate an attach failure during an unrelated operation. Table exits stay unchanged:

```json
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_not_enabled","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_capability_unsupported","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_admission_conflict","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_unauthorized","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_grant_revoked","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_binding_unavailable","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_binding_conflict","details":{}}
{"protocol":"curator-session-manager/1","ok":false,"code":"attach_in_use","details":{}}
```

Negative public controls: unknown code; any V09–V23 code in an otherwise valid fd4 or JSON envelope; modified message/version/type/details; seventh fd4 key; duplicate keys; trailing documents. All refuse without reflection. Test both private four-member frames and forged public-shaped frames carrying a private code, so shape failure does not mask an accidentally widened registry. Decoder vectors alone do not establish pre-spawn, grant or native correctness.

## Appendix B. Exact host-private refusal vectors — V09–V23

These closed four-member results are private `host-debug-attach/1` records, with no public schema identity, public message member, launcher exit or fd4/JSON registry membership. Fixed local display text and exact triggers are the private table in C06. Each vector is one JSON object plus LF. They remain private even if the viewer closes. V23 is the explicit independent-rights denial added by the host contract.

V09 — private `input_writer_required`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_writer_required","details":{}}
```

V10 — private `input_busy`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_busy","details":{}}
```

V11 — private `input_observation_stale`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_observation_stale","details":{}}
```

V12 — private `input_fenced`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_fenced","details":{}}
```

V13 — private `input_stopped`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_stopped","details":{}}
```

V14 — private `input_invalid`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_invalid","details":{}}
```

V15 — private `input_conflict`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_conflict","details":{}}
```

V16 — private `input_capacity_exceeded`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_capacity_exceeded","details":{}}
```

V17 — private `input_storage_unavailable`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_storage_unavailable","details":{}}
```

V18 — private `input_effect_unknown`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_effect_unknown","details":{}}
```

V19 — private `input_operation_unsupported`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_operation_unsupported","details":{}}
```

V20 — private `interaction_unsupported`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"interaction_unsupported","details":{}}
```

V21 — private `interaction_stale`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"interaction_stale","details":{}}
```

V22 — private `interaction_owner_required`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"interaction_owner_required","details":{}}
```

V23 — private `input_right_required`:

```json
{"contract":"host-debug-attach/1","ok":false,"code":"input_right_required","details":{}}
```

Private decoder controls refuse unknown codes, public-envelope substitution, extra keys/details and mismatched contract/version without reflecting content or creating effects. Scope/rights/lease/terminal gating must still be tested at the actual operation, including minimal legitimate grants; refusing every request cannot pass.
