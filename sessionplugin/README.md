# Session plugin engine

This package owns durable operation admission, replay detection, cancellation,
resume selection, pagination, generation fences and provider control. Provider
execution enters through `Host`, with optional resume and control interfaces.
No requested environment, credentials, prompt or native plan is persisted.

The pinned Curator schemas remain the published wire authority. The request DTO
contains neutral execution and selection fields. A consumer extension is decoded
and interpreted by that consumer's adapter while the original validated bytes
remain the replay identity. `Engine.Manager` and `Engine.Capabilities` allow the
embedding consumer to retain its published identity and advertisement.

## Tools

Use Go and its matching formatter from the repository toolchain:

```sh
gofmt -w sessionplugin/*.go
go test -p 1 -parallel 1 -count 1 ./sessionplugin
go vet -p 1 ./sessionplugin
```

Tests use temporary directories for ledgers. They exercise deterministic host
fixtures; they do not qualify a live provider or daemon.
