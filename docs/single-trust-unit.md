# Explicit first-dogfood runtime profile

Implementation candidate; independent implementation review remains required.
Base: Host `024fb07165dc45e5e1ef3a86f60716a32848f8cf` (cleanup-r2).
This implements the selected shared-UID runtime profile, plain pathname backend
and configuration-bound operator policy described below.

`single-trust-unit` means **shared UID, no per-agent isolation**. Service and
caller are the current unprivileged OS principal (S=C); individual agents and
their children are trusted together. The constructor accepts no caller UID and
refuses root and differing real/effective UIDs. File modes, held descriptors and
hash checks are optimistic observations within that trust unit. They supply no
custody boundary against the principal or its other processes. This profile is
never production admission. Production O/S/C principals, root-owned 0440
service-group custody, separate ingress authority and the child UID-switch
helper remain separate production prerequisites; Host remains unprivileged.

The ordinary product binary reports this posture without starting anything:

```sh
swarma-session-host profile --name single-trust-unit --backend plain-pathname-v1
```

The JSON includes actual service/caller UIDs, `per_agent_isolation:false`,
`production_admission:false`, `atomic_pathname_exec:false`,
`sealed_procfd_qualified:false`, custody limits and the outstanding cutover
requirements. This command describes an explicit selection; it does not attest
an installed launcher, an active runtime, a Source origin or a process identity.
Omitting either selection or requesting production or sealed procfd refuses.
It does not activate the profile on the existing `serve` route.

The private composition requires `privateRuntimeProfileConfig` with explicit
name, backend and policy path. It constructs its own policy instead of accepting
an arbitrary policy dependency. Only `plain-pathname-v1` is implemented here:
the existing ordinary launcher/runner path, with fixed argv derived from the
once-retained launch request. Scripts and dynamic executables remain eligible
under the existing physical checks. There is no static-ELF first-run gate,
procfd execution or claimed sealed qualification. Failed selections never fall
back to this profile. This candidate does not implement a production mode.

The launcher is checked by canonical path, retained file/parent identity and
SHA256 before Start. Runner still executes that pathname. The final check and
exec are **non-atomic**. A same-UID writer can change files after observation;
an interpreter, dynamic loader and the launcher's downstream native executable
are outside the launcher hash. Existing actual-launcher qualification must be
supplied by the integrating owner; inert script/process tests here establish
this composition's plain backend behavior only. They do not qualify a real
Curator/native installation or justify any claim about sealed procfd.

Policy version 2 is canonical JSON (exact `json.Marshal` field order plus LF):

```json
{"version":2,"runtime_profile":"single-trust-unit","backend":"plain-pathname-v1","selected_configuration_sha256":"<64 lowercase hex>","grants":[{"profile":"swarm-orch","lock_sha256":"<64 lowercase hex>","exec_profile":"plain-pathname-v1"}]}
```

`freezePrivateProfile` copies and normalizes the selected child environment,
copies the Core environment and hashes an explicit JSON projection of profile,
backend, policy path, record/state/socket paths, launcher path and SHA256,
child/Core environments, provider/model/effort, native release, permission and
I/O/poll/readiness bounds (nanoseconds). The selected digest is exposed on the
private runtime report and in the synthetic executable's `runtime_profile`.
It binds configuration values; it is not a content hash for every referenced
file or an authentication proof. The integrator must generate the policy from
that same private selection. Environment order is normalized by the existing
allowlist parser. Current Core output still traverses the existing correlator.

The policy grants the retained **record** profile/lock to the one fixed exec
profile under that digest. Request-only substitutions do not select grants.
Actual manager launcher path, pin, record path and environment must match the
frozen selection at both policy checks. Existing strict size/encoding, ownership,
ancestor, mode, identity, byte-pinning and terminal-loss checks remain effective.
Version 1 policies cannot admit this profile. The older distinct-caller private
policy candidate retains its same-UID refusal and supplies no production route.

Source's real provider-origin factory and installed ACK contract, plus Linux v2
connected-peer/process identity qualification, remain required before actual
cutover. This change supplies no replacement adapter, invented ACK or Linux
identity. The tagged fixture's Core/provider/enrollment/ACK and inert native
process remain synthetic. Runner v2 remains a separately owned component; no runner files are changed by this profile.
Cleanup-r2 custody/no-replay/fresh-only behavior is retained. No existing session,
tmux process, deployment or main branch is switched. Independent exact-candidate
review, integration and Source signed landing remain required.
