# Host configuration defaults

`FileConfig` implements `sessionmanager.HostConfig` by reading its explicit `Path` on each snapshot. Composition sets `Optional` only for an implicit default path; a selected missing file and every unreadable/malformed file refuse. No board package is imported and no board configuration discovery is performed.

The relocated host keys are `session_manager.providers.claude.goal_ack_timeout`, the Codex `sleep_tool`, `goal_token_budget` and `auto_continue` settings, and context clearance/builder allowlist/payload mode. Timeout absence is 30 seconds, sleep is enabled and the default token budget is 50,000,000. Local payload mode defaults off, with no restricted clearance grant. Unknown or duplicated consumed keys refuse before settings are returned. Provider defaults beyond the current `HostSettings` port remain available from `Load` for real launch construction.

`ParseContextRetentionConfig` preserves the original nine optional retention knobs, positive duration/count validation and day suffix. `Load` returns both validated `Retention` values and original `ContextRetention` bytes. Unset knobs remain unset so the context runtime owns their defaults. Unknown and duplicated knobs refuse before usable settings are returned. Parsing does not apply a retention policy. The current host settings contract has no retention field, so `Snapshot` still refuses configured retention with `ErrContextRetentionSupplierRequired` until the real context request/default path consumes it. File discovery, complete launch-plan binding and full serving composition remain required.

`NewMaterializationHostCodec` adapts the actual legacy provider subrecord decoder to the context catalog with an explicit lifetime context. It preserves cancellation and active-state refusals; it supplies no materialization authority or unsupported native restoration.

Run `go test -p=1 -parallel=1 -count=1 ./hostdefaults`, `go build -p=1 ./hostdefaults` and `go vet -p=1 ./hostdefaults`. Tests use isolated selected files; they do not read installed configuration or start providers. Store local logs under `.temp/`.


`NewLegacyAttachHandler` decodes the full original control request and returns
its original attachment projection, including the actual session-owned proxy
and client metadata. `OpenStandalone` mounts it inside the existing authenticated
ControlServer. `LegacyAttachSource.WithVerifiedAttach` is an explicit trusted
raising-side boundary: verify the complete original plan, location, current
holder, launch/adoption/materialization rights and configured environment for
the callback lifetime. No production registrar is supplied. Request owner,
RuntimeControl data, PID and shared user do not grant rights. The handler captures
the real owner lease before verification and passes that exact comparison to
Host.Attach; a zero comparison asserts absence under the host reservation.
Unknown or replaced slots refuse before launch. Source callbacks expire and
cannot issue duplicate attachments. Unsupported original owner-run and explicit
Codex budget consumers remain refusals until their real source is connected.


`BuilderShimCommand` and `BuilderShimArgs` explicitly select the actual installed
helper contract. The executable composition supplies its own real artifact and
`session context-gateway-shim` args. Without selection the library retains an
unavailable builder runtime; orphan args or relative commands refuse before
runtime state. With selection, `Standalone.BuilderGateways` is the same runtime
in `sessioncontext.Options.BuilderGateways` and `HostOptions.BuilderGateways`.
Host.Close finalizes it before provider observation retirement, including the
ControlServer's deferred close during serving/drain. Standalone.Close reads the
host's retained finalization result, then closes the budget factory and cancels
its lifetime. A failed observation remains a refusal on repeated close; an empty
runtime cannot erase it. Actual Start-before-launch/Bind-after-launch, native
observation and canonical publication suppliers remain required. No background
race, synthetic completeness report or guessed signature fills those gaps.
