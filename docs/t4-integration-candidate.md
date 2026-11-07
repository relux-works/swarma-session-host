# T4 private integration proposal — synthetic qualification only

Continuation: [the private runtime candidate](t4-private-runtime.md) now supplies
a concrete lifecycle and tagged Linux executable. The cross-repository helper
uses that lifecycle. The original bounded glue and custody description below
remains applicable; production Source, caller admission and restart adoption
remain unselected.

This is a module-owned private integration proposal. D1,
D3, D4 and T2 are approved composition decisions. T1's completed immutable
receipt identifies `d3f79e195aa6cd2756124e6227c2ae6a1f56a07c`, following
`dca09a0c149aad143e450cf11d08e66db70dc57e`; its proposed versioned operator
policy encoding remains subject to integrated review. No caller DTO is policy.

## Retained creation and cleanup

Private `ordinaryCreateHandler.transferTo` serializes observation and exactly
one T2 acknowledgment with create/cleanup. The same ordinary operation,
pre-child claim, retained member/record/workspace, actual runner and continuing
Host remain held. A failed acknowledgment keeps T1 cleanup duty. A successful
acknowledgment releases T1's duty without canceling or closing physical owners;
T2 then owns the joint cleanup under the original finite lifetime. T1 closes its
policy-file handles because they gate the completed one-time Start.

Close cancels the original owner before waiting on the create lock, including
while Start is in flight. Its watcher chooses T1 cleanup or joins the acknowledged
T2 completion; it never independently closes transferred handles. A canceled
waiter cannot abandon cleanup. The final T1 completion follows the T2 join.

Private `openCreateNotificationManager` combines that transfer with the existing
manager. Its admission checks the actual acknowledged correlation, actual Host,
operation kind and exact retained bytes. The existing independent configured
record/Core/profile/workflow and broker checks still apply. The continuing record
source is separately retained by the continuing Host; the original pre-child
physical source remains retained and rechecked by T2. An absent broker refuses.
Public `manager.Open` remains unchanged and closed for ordinary operation
admission. The continuation installs the private glue only in explicit private
composition, with injected trust and Source dependencies. No native/provider runtime is activated here.

## Executable qualification

`TestT4AcknowledgedCreateHandoff`, `TestT4FailedTransferKeepsT1Duty`,
`TestT4TransferSerializesWithCleanup`, and
`TestT4UnselectedBrokerKeepsCreateCustody` exercise the glue. The fixture executes
only the Go test child behind a pinned inert wrapper, through actual T1
operator-file checks and runner.Start. Host's actual Codex parser observes the
synthetic initialize/thread/start output. Its actual controller generation and
full launch plan feed T2 correlation. Core emitted documents and provider origin
remain explicit deterministic fixtures, checked by the existing correlators.
The distinct caller UID in the operator policy is a synthetic composition
assumption; this shared same-UID host supplies no production caller isolation.

Compile this package as a test binary and pass its absolute path as
`T4_HOST_TEST` to the paired Source test on PR175 plus T3 `5f172b2`:

```sh
# Host repository
GOMAXPROCS=2 GOFLAGS=-p=2 go test -race -p 2 -c -o /tmp/host-t4.test ./internal/sessionhost
# Source repository
T4_HOST_TEST=/tmp/host-t4.test GOMAXPROCS=2 GOFLAGS=-p=2 go test -race -p 2 ./internal/hostnotification -run '^TestT4CrossRepository$' -count=1 -v -timeout 120s
```

The opt-in `TestT4HostHelper` is test-only. Its inherited Unix STREAM endpoints
and bounded fixture control exchange join the existing manager/broker and Source
WithConnection callback. The Source test retains its real disk-state enrollment,
consumer callback, generation, receipt, adapter and ledger. Only actual Source
generation/receipt complete the frozen Host binding; the installed handshake
checks that complete binding before any method. The fixture handshake is **not**
a proposed upstream ACK encoding, source-origin factory or production grant.

Cases prove acceptance, durable external-fence refusal, dropped reply after
actual Host acceptance, installed-ACK refusal, wrong Source generation, Source
revocation and actual runner/controller loss. Host journal reload and Source
ledger reload are checked after joined cleanup. A lost response leaves one Host
intent and Source UNKNOWN; reload cannot abandon or retrigger it. No mailbox
cursor advances. Each owned helper is reaped. Existing T2 controls separately
refuse restored/replayed native evidence; durable Host restart/adoption is not
implemented or claimed by the Source-ledger reload test.

## Review and adoption boundaries

The candidate retains the private notification lifecycle, manager admission,
T1/T2 handoff and both documented fixture interfaces. Unfinished implementation
remains unreviewed. Qualification evidence does not supply an independent review
verdict. Original signed inputs and exact blob mappings are retained separately
in local review evidence.

A usable synthetic candidate is different from an installed ordinary route.
Remaining external selections are the actual Source provider-origin factory /
connection-server predicate and composition location, its installed-ACK mechanism
and failure ordering, and the selected Host dependency. No such formats or
permissions are inferred here. Production also needs proof of the T1 caller /
operator / exec-profile trust boundary, integrated independent review, Source-owner
exact-head review and authorized platform qualification. Real Linux identity
paths remain unsupported where the current implementation says so; inert test
children do not qualify them. This candidate authorizes no landing, deployment,
native or provider execution. Separate acceptance of the SH1 component does not
accept the integrated candidate or change original signed inputs.
