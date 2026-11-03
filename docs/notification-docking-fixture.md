# Inert notification docking fixture

The opt-in `TestSourceNotificationDockHelper` runs the private Host notification
stream and journal against synthetic adapter metadata. It starts no scheduler,
process descendants, native runtime or workflow. It adds no product attachment
API. Its caller supplies an independently implemented broker client fixture.

Build the test executable from an exact source revision on macOS:

```sh
mkdir -p .temp/prototype/bin
CGO_ENABLED=1 GOMAXPROCS=2 GOTOOLCHAIN=local GOWORK=off \
  go test -p 1 -c -o .temp/prototype/bin/notification-dock.test ./internal/sessionhost
```

The fixture parent verifies the executable and source digests before starting
that exact executable with these fixed arguments:

```text
-test.run=^TestSourceNotificationDockHelper$ -test.count=1 -test.timeout=20s
```

Set only `HOST_NOTIFICATION_DOCK_HELPER=1` in its environment; supply inherited
Unix STREAM endpoints as FD3 (notification wire) and FD4 (fixture control).
Neither endpoint is passed to another process by the helper. Without the marker,
the test skips; a skip is not evidence of docking. With the marker, missing or
invalid endpoints fail. The parent records the child PID, closes its endpoints
on failure, and terminates and reaps only that owned child if necessary.

Use an overall parent deadline of 20 seconds per case, with setup/ready and final
result/reap each bounded by 2 seconds and the same earlier overall deadline.
The actual notification service uses a 2000ms call budget; idle waiting consumes
no call budget. Bound captured stdout and stderr to 8192 bytes each. The control
stream carries the fixture result; ordinary Go test output is not that result.

Control frames are UTF-8 JSON objects followed by LF, at most 8192 bytes including
LF. Setup rejects unknown or duplicate decoded keys, wrong case, null, invalid
binding fields, unsupported cases and any call timeout other than integer 2000.
The setup object has exactly these fields:

```text
contract: "source-host-dock/1"
kind: "setup"
case: "accepted-lookup" | "input-not-ready"
binding: complete curator-host-notification/1 twelve-field binding
call_timeout_ms: 2000
```

Create the broker fixture's actual isolated enrollment before sending setup.
The helper validates the complete binding and creates matching synthetic Host
metadata before installing it. It does not authenticate or inspect the caller's
state. Workflow references and digests obey the existing wire predicates; the
helper neither resolves nor executes them.

The ready frame has exactly `contract`, `kind:"ready"` and the unchanged
`binding`. Exchange actual notification frames over FD3. Close the original
wire endpoint after the case. After its service goroutines stop, the helper
reads its own durable journal and sends a final frame with exactly:

```text
contract, kind:"result", case, binding,
intent_count, notice_id, sequence, native_stage, wake_count,
binding_active, input_fenced
```

`accepted-lookup` requires one retained intent, its original notice and sequence,
and one wake. Same-attempt lookup and retry on the original usable endpoint must
preserve them. `input-not-ready` uses the actual busy-input gate and requires
zero intents and wakes, with empty notice, sequence and stage. Cleanup must leave
the binding inactive and input fenced. The wake recorder has 32 slots and no
scheduler, so duplicate signals cannot hide behind the production channel's
coalescing. Its count does not prove native dispatch or execution.

Loss-before-reply coordination is not implemented by this helper. There is no
clean private barrier between actual durable acceptance and reply writing.
The helper refuses the `lost-reply` case; separate existing Host loss tests do
not establish that cross-repository case. It never reattaches a lost endpoint.
Neither these fixtures nor a successful client docking prove installed manager
admission, enrollment authority, native execution or idle message delivery.
