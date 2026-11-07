# T1 contract — D2 operator-file candidate, uninstalled

Continuation of signed T1 dca09a0c149aad143e450cf11d08e66db70dc57e. Original WIP base remains
972bccdc71b2191a8ce0ac498165d8744a98e227. D1/D3/D4/T2 are approved composition
decisions; this candidate supplies the authorized first D2 implementation. Encoding
below is a proposal for integrated independent review, not a landing or
production-adoption decision. This local signed candidate authorizes no
landing, deployment or live configuration.

D1 composition
--------------
Private openOrdinaryCreateHandler(parent, expires, records, launcherConfig,
socket, timeout, policy createPolicy) owns a successfully supplied policy until
cleanup. On constructor failure its caller retains policy cleanup responsibility.
Nil selects unselectedCreatePolicy (always refuses). No listener, public policy
registration, installed caller, CLI or provider entry is added. Existing fixed
launcher binary/digest/environment and socket/timeout remain composition-owned.

D2 exact seam and first implementation
--------------------------------------
Private createPolicy has checkCreate(context.Context, *ordinaryWireOperation)
error and close(). Composition chooses the implementation; no wire body,
caller text, expected DTO, executable digest or native/yolo mode is authority.
A future dispatcher-permit implementation can implement this same seam; none
is added here. No permissive default or public callback is added.

newOperatorCreatePolicy accepts private composition configuration:
- path: absolute clean symlink-free path, never read from request/environment;
- execProfile: ID of this composition's fixed executable/environment policy;
- operatorUID: expected file owner, root or this process's effective UID;
- callerUID: independently authenticated OS caller UID, distinct from root,
  operatorUID and this process's effective UID. This value is composition data,
  never an expected request DTO. It does not itself authenticate anyone.
The constructor refuses known shared-UID/privileged/unconfigured boundaries.
It cannot prove that a composition-supplied caller UID is true. This candidate
has NO production caller authentication or installed route. Production adoption
must REFUSE until the actual caller isolation, no additional caller capabilities
or operator filesystem control, and composition/exec-profile binding are proven
by integration and independent review. Synthetic distinct UID values in tests
prove only the adapter mechanism. They are not proof of OS isolation here.

The file must be regular, non-symlink, one hardlink, exactly 0400 or 0600, with
no special mode bits, owned by the selected operator UID, 1..65536 bytes.
Every ancestor must be a root/operator-owned real directory without group/other
writes; root-owned sticky directories such as /tmp are allowed because distinct
unprivileged callers cannot replace operator-owned children. Retained ancestor
and file identities and named/held observations are checked before/after reads;
O_NOFOLLOW/O_NONBLOCK plus bounded read prevent observed link/FIFO substitution.
Unexpected owner, mode, link count, path, ancestry, read or parse result refuses.

VERSION 1 ENCODING PROPOSAL (not deployed)
Exactly Go json.Marshal of the following field-ordered typed structure, plus
one LF; UTF-8, no extra whitespace, alternate escapes, trailing documents or
field ordering. Example below uses a synthetic lock; it is not a live config:

{"version":1,"grants":[{"profile":"swarm-orch","lock_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","exec_profile":"inert-v1"}]}

version is exactly integer 1. grants is a nonempty array of at most 128 entries.
Each entry has exactly profile, lock_sha256, exec_profile in that order.
Profile and exec-profile IDs match ^[a-z][a-z0-9_-]{0,63}$; locks are exactly 64
lowercase hex digits. All unknown, missing, duplicate, case-folded or null fields
and alternate encodings refuse through exact re-encoding equality. Duplicate
(profile, lock_sha256) pairs invalidate the whole file, even with the same
exec-profile ID or when unrelated to the requested operation. No wildcard,
inheritance, merge, refresh or fallback exists. Entry order is not authority.
An empty list refuses all; it is rejected rather than treated as configuration.

Before selecting a grant, the adapter rechecks the SAME ordinaryWireOperation,
its exact manager/preChildOperation, complete launch bytes, retained record,
workspace and source. The lookup pair comes from retained record.Profile and
record.ProfileLock, whose match with the actual operation was established during
physical retention. The incoming expected profile/lock is never used as an
independent grant. A unique exact pair must map to this composition's execProfile.
The grant cannot select a new binary, argv, environment or native mode. No
external exec-profile resolver or public registry is asserted by this candidate.

At first successful check, pin actual operation pointer, open file/parent,
ancestry identities, full file SHA-256 and matched grant. Second check immediately
before Start reobserves exact physical operation and file ownership/identity/
bytes; any observed drift is terminal refusal, including same-byte replacement
or same-size content drift with restored mtime. Failed checks close policy file
handles and cannot be repaired/retried. A handler owns one policy instance; it
cannot borrow that instance for another operation or another handler.

0600 DOES NOT protect against same-UID host agents. Shared agents on this host
are NOT OS-isolated: they belong to the trusted operator boundary for filesystem
purposes. Trusted operator controls policy, composition, executable and config
ancestors; untrusted callers must be independently isolated and unprivileged.
These are optimistic current-file observations, not a filesystem writer lock:
unobserved operator ABA changes, or changes after the final check, cannot be
made atomic with runner.Start. No claim of protection from a malicious operator,
same-UID host agents or compromised host is made. Production adoption remains
closed/unproven on this host. No real policy/config deployment is performed.

D3 one attempt and cleanup (preserved)
-------------------------------------
ordinaryCreateHandler.create serializes create/close. Only launch qualifies.
The first policy check precedes takeRunnerConfig, which consumes the SAME
pre-child attempt before final physical/executable and second policy checks.
Final policy denial terminally retains refused; no repair retries. Starting is
recorded before the only runner.Start. Request cancellation before Start refuses;
after Start it cannot abandon a possible returned runner. Continuing owner loss
closes the actual child and policy handles and joins wire cleanup independently
of canceled close waiters. Physical owner loss may independently close retained
record/executable resources. No filesystem atomicity is promised.

D4 observations/recovery (preserved)
------------------------------------
Private in-memory outcomes: refused, starting_unknown, runner_owned,
start_unknown, closed, cleanup_unknown. These are not public wire/native/
enrollment codes. Exact duplicates observe the current outcome without a second
Start; conflicting bytes refuse. Failed/ambiguous Start never retries. No durable
recovery/replay or PID/socket adoption is supplied; process loss requires future
owning-composition custody/reconciliation. Candidate stays uninstalled.

T2 exact custody alignment (no transfer installed)
-------------------------------------------------
The T2 handoff contract expects
retainNativeCorrelation(op, server, continuing), once-only and private. T1
custody() after runner_owned still returns the exact *ordinaryWireOperation,
*preChildOperation, *retainedMemberOperation, *memberRecordSource, *runner.Server.
No reopen, copied DTO, PID reconstruction or substitute handle is introduced.
The T2 candidate at 71dca16 supplies explicit
c.acknowledge(theSameOp, theSameServer, theSameContinuingOwner). Future T1 docking
must serialize construction/acknowledgment with this handler's create/close and
release cleanup DUTY only after acknowledgment returns nil, without closing or
canceling the transferred actual owners. Failed construction/acknowledgment
leaves cleanup with T1. The finite preparation lifetime continues after handoff.
These remain borrowed observations under T1 cleanup in THIS unit. It adds no
transfer/release implementation and changes no T2 source. Native/provider/
Source admission and notifications remain closed. No end-to-end T2 integration
or independent review acceptance is claimed.

Next decision: integrated independent review of this minimal VERSION 1
encoding and proof of the real composition/caller/exec-profile trust boundary;
then integrate exact-handle acknowledged T2 custody under the owner's holds.
