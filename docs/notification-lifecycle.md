# Private notification attachment lifecycle

The Host owns one notification lifecycle per execution. Its private manager
facade validates its owning manager before allocating a close-on-exec Unix
STREAM pair. A second preparation cannot allocate or replace the first pair,
even after loss. The broker endpoint transfers once, only after the actual Host
execution, controller epoch, runner instance and observed native thread are
available. Public preparing status includes only attachment and fixed workflow
references; it contains no partial binding.

The completion path requires a live broker enrollment check, the same admitted
manager, the original attachment, all twelve binding fields and an unexpired
lease. Binding syntax alone is insufficient. Installation and terminal state
changes share the Host mutex. The service may run after successful installation;
the broker must still complete its own installed acknowledgement before using
methods. The callback does not wait for methods or that acknowledgement.

Preparation/custody expiry, broker lease expiry, cancellation, stream loss,
changed Host identity and persistence failure close the lifecycle. Closure joins
owned service work outside the state mutex. No terminal lifecycle becomes ready
again. Durable notification obligations remain available for reconciliation;
loss cannot establish non-application. Busy input may retain an admitted
attachment while Trigger independently refuses new input.

The private continuing-owner core opens and retains the actual Host controller
under an explicit finite custody deadline. Individual status request cancellation
leaves that owner intact. Owner cancellation, expiry or explicit Host closure
joins its services and releases its controller without stopping the runner.
Current-object checks reject a copied Host, changed execution/epoch/runner,
stop or storage failure; observed loss is terminal. Canceling a close waiter
does not abandon cleanup. The owner core supplies resource ownership only;
it cannot implement member admission or activate a notification attachment.

The manager and broker admission checks currently have private interfaces and
inert test implementations only. They are not public proof constructors, request fields or
production admission. The actual continuing selected-manager owner, broker
adapter, designated child transfer and productive native use still require
integration and qualification. This local lifecycle alone does not make a
session launchable through the Host.

## Verification

Run on macOS with the project's Go toolchain and cgo available:

```sh
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go test -p 1 -race -count=1 \
  -run '^TestContinuingOwner|^TestManagedNotification|^TestNotificationStream|^TestNotificationJournal' \
  ./internal/sessionhost
GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off go vet -p 1 ./internal/sessionhost
```

These tests cover local one-shot ownership, correlation, lease/cancel loss,
completion/terminal races, retained intents and closed Trigger gates, with
nearby valid controls. Descriptor checks verify actual CLOEXEC metadata only;
they do not qualify designated child inheritance or native cleanup. The existing
stream tests cover bounded exchange, framing and lost-response obligations.
Continuing-owner tests open an actual local Host against an owned inert Go
child, verify controller release and preservation of that runner, and reject
foreign Host objects beside a valid current owner. No test here establishes
real member admission, Source enrollment or productive native execution.
