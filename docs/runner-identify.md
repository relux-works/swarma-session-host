# Read-only runner identify exchange

The runner implements a separate runner-identify/1 ownership exchange on its
existing private Unix endpoint. Controller protocol 3 and write-receipt
protocol 1 retain their request/response forms and control behavior.

The first qualified runtime is Darwin/arm64 with cgo. Linux identify peer proof
is explicitly unsupported; successful cross-compilation is not runtime
qualification. Darwin without cgo cannot capture the required child birth.

This exchange reports a point-in-time ownership snapshot. It does not establish
that the child is live, pin its lifetime, grant control or perform hosted
admission. The child is the directly owned process; no descendants are searched
or adopted. Same-UID authentication identifies an OS user, not distinct callers
sharing that user.

## Closed frames

An exchange uses a fresh connection, one UTF-8 JSON object followed by LF, at
most 1024 encoded bytes including LF. It processes only the first request,
writes at most one response and closes. Pipelined control is never dispatched.

A request has exactly these three members:

    {"schema":"runner-identify-request-v1","execution":"example-execution","expected_instance":"0123456789abcdef0123456789abcdef"}

Execution must equal the immutable Config.Execution supplied at creation. It
is nonempty, well-formed UTF-8, at most 128 decoded bytes, excluding U+0000–001F
and U+007F–009F. Comparison uses exact decoded UTF-8 bytes, with no trimming,
case folding or Unicode normalization. Equivalent JSON escapes are permitted.
Session and operation identifiers are not aliases for this value. The expected
instance is exactly 32 lowercase hexadecimal characters.

A successful response has exactly these members:

    {"schema":"runner-identify-response-v1","execution":"example-execution","instance":"0123456789abcdef0123456789abcdef","result":"verified","reason":null,"child":{"pid":123,"birth":{"sec":1800000000,"usec":123456}}}

The child contains exactly pid and birth; birth contains exactly sec and usec.
PID is 1..2147483647; seconds 0..9007199254740991; microseconds 0..999999. Numbers
use decimal integer syntax, with no fraction, exponent or negative zero.

A refusal also carries all six response members:

    {"schema":"runner-identify-response-v1","execution":"example-execution","instance":null,"result":"unavailable","reason":"child_reaped","child":null}

| Result | Closed reasons |
| --- | --- |
| conflict | execution_mismatch, instance_mismatch |
| unavailable | birth_unavailable, child_reaped, wait_unknown, runner_closed, busy, unsupported_platform |

Refusals echo only the requested execution. Instance and child must be null.
Verified requires both, with null reason. Duplicate decoded keys, unknown or
missing members, invalid UTF-8, unpaired surrogate escapes, trailing JSON,
invalid enums/types/numbers or inconsistent result fields are rejected.
Malformed, unsupported or unauthenticated offers close without a structured
identify reply. Legacy responses, status and EOF are never ownership proof.

## Lifecycle and serialization

Original birth is captured once immediately after child creation and before
collector, reaper and acceptor goroutines. Darwin uses PROC_PIDTBSDINFO with
the owned-unreaped lookup option, validating exact structure size, returned
PID and microsecond range. Failed capture remains unavailable; it is never
repaired by rereading a remembered PID. The live/CDHASH admission observer is
unchanged and continues to reject zombies.

| State at snapshot | Result |
| --- | --- |
| Original live, owned, unreaped, birth available | verified ownership |
| Original exited, still owned and unreaped | verified ownership permitted; independent live check refuses |
| Wait consumed and reaped published | unavailable/child_reaped |
| Wait error, inconsistent result or lost handle | unavailable/wait_unknown |

Closing, unsupported capture and missing birth have their explicit refusal
reasons. Execution/instance mismatch discloses no actual instance or child.

The ownership snapshot tries processMu once, then mu once. Contention returns
busy; failed second acquisition immediately releases processMu. The path never
takes writeMu. Under both locks it copies bounded memory only, releasing mu then
processMu before encoding or network IO. It performs no kernel query, wait,
signal, output read, controller claim or receipt reservation. Ordinary reap and
stop retain their existing authority and lock order. Transport registration and
cleanup retain the existing connection-map synchronization.

A snapshot may precede reap even when its reply arrives afterward. No network
acknowledgement delays reap or pins ownership.

## Bounds and peer evidence

The server authenticates same-UID credentials before parsing/disclosure.
Identify has two classified nonqueued handler slots, released on every exit.
Excess handlers close. Its deadline is no later than the earlier of connection
acceptance plus 1000ms and acceptance plus the configured non-controller IO
timeout. Classification cannot start a new budget. Expired evidence is dropped.

Before classification, the existing 64-KiB frame, configured IO timeout
(at most 60s) and total-eight connection bounds remain. An ordinary controller
retains its established follow-up IO behavior; adding identify does not shrink
the legacy frame limit. Resource exhaustion does not grant a control fallback.

The unexported internal qualification client requires a caller deadline, caps
its attempt at 1000ms from starting endpoint checks, and makes one connection and
one offer with no retries. It checks the absolute socket path, private parent,
owner/mode and absence of a final symlink. It obtains UID and PID on the same
connected descriptor. Darwin uses LOCAL_PEERCRED and LOCAL_PEERPID, checking
credential version/length/group count and positive PID length/range. Expected
runner PID/UID come from the caller, never the socket reply. Path checks do not
pin the whole ancestor chain, and PID equality does not establish an incarnation.
Cancellation closes only this fresh connection.

## Qualification and remaining verifier obligations

Run from the repository root on macOS with Go 1.25.5 and the Apple C toolchain:

    CGO_ENABLED=1 GOTOOLCHAIN=local GOWORK=off go test -p 1 -run 'TestIdentify|TestPeerIdentity' ./internal/runner ./internal/localipc
    CGO_ENABLED=1 GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=2 go test -p 1 -race -count=1 ./...

Tests use owned synthetic children/private sockets. They cover closed-codec
neighbors, original tuple and refusal states, lock contention/release,
controller/receipt/output preservation, input-effect counters, slot bounds and
cleanup, cancellation/deadlines, actual same-socket UID/PID and exact post-reply
live-read order. A gated reply explicitly demonstrates normal stop/reap progress
outside the snapshot locks; its small bytes are not claimed to fill a Unix
socket buffer. Wrong-UID and metadata-owner fixtures are declared seams, not
actual cross-UID OS qualification.

The optional TestIdentifyOldRunnerCompatibility qualification takes
SESSION_HOST_OLD_RUNNER as an explicitly supplied retained pre-identify binary.
It starts only an owned synthetic runner/child, checks identify unavailable,
then checks continued protocol 3 control. Without this input it is skipped;
ordinary tests do not require a historical checkout. No hosted process is
upgraded or restarted. Test output is on stdout; optional
SESSION_HOST_TEST_RESOURCES writes owned PID/path receipts to the named file.

The qualification client is unexported in internal/runner and has no command
caller. Its success is an ownership snapshot, never an admission verdict.
No production verifier, closed manager-record amendment or public deployment
capability advertisement is introduced.

Each real launcher/observer verifier must independently consume the SAME
authoritative immutable runner.execution equal to Config.Execution, plus
instance and endpoint. The current external closed record still needs its
accepted amendment; missing correlation cannot be inferred from a session,
operation, private journal or status response.

Each verifier must separately check the full runner boot/PID/UID/birth/image
around the exchange. Only AFTER complete valid reply decoding may it begin a
NEW independent child live/incarnation read, comparing the original child tuple
and the full admitted boot/UID/birth/image. The read must finish within the SAME
original 1000ms verification deadline. Cached, pre-reply, early-started or late
reads cannot qualify; no new deadline starts after the reply. The tests exercise
this ordering with owned processes and a test-only verifier; neither actual
external record reader is qualified here. Even a successful fresh live read
cannot promise continued liveness immediately afterward.
