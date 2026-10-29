# Neutral session runtime

`NewHost` owns controller fencing, provider handles, attachment access and the
sole record writer. Its codec and authority ports keep canonical application
interpretation outside this package. Provider factories receive typed launch
payloads and typed prior images; they validate and seal native launch plans.

`NewControlServer` serves authenticated Unix HTTP using that host's lifetime
lock. It publishes the existing endpoint identity, checks it through the real
status route before becoming ready, and joins shutdown. The supplied HTTP
handler converts application wire requests to typed host calls. It must encode
`ControlServer.Status()` identity fields unchanged. The server handles bearer
authentication and acknowledged drain itself.

This library does not by itself supply a standalone executable, a legacy record
codec, or canonical authority across a process boundary. Materialization-backed
attach and the remaining application control routes require their actual
integration; selecting an empty authority implementation does not preserve them.

## Tools

Go 1.26.8 builds and checks this package. Narrow control-server fixtures run with:

```sh
GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=2 go test -p=1 -parallel=1 -count=1 \
  -run '^TestControlServer' ./sessionmanager
```

The fixtures create and remove their own temporary files. If `TMPDIR` is too
long for Unix sockets, set `SESSIONHOST_TEST_SOCKET_ROOT` to an existing, owned,
short task directory under `.temp/`. Only fixture socket directories use that
setting; it does not configure the production server. Test output goes to stdout.
