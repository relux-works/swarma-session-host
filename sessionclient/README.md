# Authenticated local client

`sessionclient` owns the existing Unix socket HTTP control transport and raw
terminal stream. It creates no daemon, launches no provider and interprets no
board or goal authority. Request and response DTOs remain with the consumer.

`Dial(EndpointFiles)` reads the recorded endpoint and token with the existing
0600 requirement and checks the caller's opaque namespace. `Connect` also checks
the existing status route, protocol and startup health without starting a host.
The historical `board_fingerprint` metadata key, protocol version 8, route bytes,
terminal preamble and established diagnostic strings remain compatible.

`DoRequest` marshals ordinary JSON requests and decodes responses. Its final
boolean selects the ten second default deadline; an explicit caller deadline
always takes precedence. Long operations pass `false`. Consumers with stricter
response policies use `RoundTrip` and must close the returned response body;
closing also releases a transport-created deadline context. This raw interface
does not validate or authorize the payload.

`OpenTerminal` uses a dedicated authenticated connection and preserves buffered
terminal bytes after the preamble. The context bounds dialing; established
interactive reads retain caller ownership through `SetReadDeadline` and `Close`.
The legacy manual handshake does not gain HTTP transport cancellation semantics.
`ResizeTerminal` sends the existing geometry payload. Detach remains a control
request with the consumer's existing route and attachment identifier.

The package uses only the Go standard library. From the module root:

```sh
go test -p=1 -parallel=1 -count=1 ./sessionclient
go build -p=1 ./sessionclient
go vet -p=1 ./sessionclient
```

Tests use owned Unix/HTTP fixtures and in-memory transports. They exercise
positive requests, namespace and mode refusal, typed errors, decoder skew,
deadline ownership, protocol compatibility and terminal cleanup. They do not
qualify a live daemon or a provider. Temporary fixture sockets are removed by
test cleanup; command logs belong in the caller's temporary evidence directory.
