# go-socketio-v4

A Socket.IO v4 server for Go, written from the official protocol
specifications and validated against them — the official Engine.IO
compliance suite (24/24 passing) and the real `socket.io-client` v4
JavaScript client (11/11 interop scenarios passing).

## Install

```sh
go get github.com/somprasongd/go-socketio-v4
```

The only dependency is `gorilla/websocket`.

## Quick start

```go
package main

import (
	"log"
	"net/http"

	"github.com/somprasongd/go-socketio-v4"
)

func main() {
	srv := socketio.New(nil) // engine.io defaults; *engineio.Options to tune

	ns := srv.DefaultNamespace()
	ns.OnConnect(func(s *socketio.Socket) {
		s.Emit("hello", "world")
	})

	ns.OnEvent("greet", func(s *socketio.Socket, args []any, ack func(response ...any)) {
		// client did: socket.emit("greet", "marco", (reply) => ...)
		if ack != nil {
			ack("polo", args[0])
		}
	})

	ns.OnDisconnect(func(s *socketio.Socket, reason string) {
		log.Printf("gone: %s (%s)", s.ID(), reason)
	})

	// serve like any handler — polling, websocket and the upgrade dance
	// all live at the mount path
	http.Handle("/socket.io/", srv)
	log.Fatal(http.ListenAndServe("localhost:9901", nil))
}
```

Rooms and broadcast work the way socket.io users expect:

```go
s.Join("readers")
ns.To("readers").Emit("card", data)   // everyone in the room
s.To("readers").Emit("card", data)    // the room minus the sender
s.Broadcast().Emit("ping")            // everyone except the sender
```

Acked emits from the server:

```go
select {
case res := <-s.EmitWithAck("ask-for-config"):
	// the client answered; res holds the ack arguments
case <-time.After(2 * time.Second):
	// client never answered (or the socket died — the channel closes empty)
}
```

Binary values travel as protocol attachments automatically — pass `[]byte`
anywhere in the args, at any depth:

```go
s.Emit("photo", map[string]any{"name": "x", "data": []byte{0x01, 0x02}})
```

Other namespaces:

```go
admin := srv.Of("/admin")
admin.OnConnect(func(s *socketio.Socket) { s.Emit("welcome", "admin here") })
```

## Protocol support

| Layer | Feature | Status |
|---|---|---|
| Engine.IO v4 | HTTP long-polling transport | ✅ |
| Engine.IO v4 | WebSocket transport (text + binary frames) | ✅ |
| Engine.IO v4 | polling→websocket upgrade dance (probe/flush/noop) | ✅ |
| Engine.IO v4 | server-side heartbeat (ping/pong, engine.io ≥ 6.4 semantics) | ✅ |
| Engine.IO v4 | maxPayload enforcement, session expiry, duplicate-poll handling | ✅ |
| Socket.IO v4 | default + custom namespaces, auth payload, CONNECT_ERROR | ✅ |
| Socket.IO v4 | events, acks in both directions, binary attachments | ✅ |
| Socket.IO v4 | rooms, broadcast, socket.To / Broadcast / namespace.Emit | ✅ |
| Socket.IO v4 | middleware hooks, volatile events, Redis adapter, connection-state recovery | not yet |

Disconnect reasons reach `OnDisconnect` in socket.io's vocabulary:
`io client disconnect`, `io server disconnect`, `transport close`,
`ping timeout`, `parse error`, `server shutting down`.

## Testing

```sh
go test ./...            # unit + golden protocol tests + e2e + JS interop
go test -race ./...      # the concurrency suites
make compliance          # the official engine.io-protocol test suite (24/24)
```

The interop tests need `node` (v18+) with `socket.io-client`; they skip with
an explicit reason when it is missing. `compliance/run.sh` runs the suite
copied verbatim from
[engine.io-protocol/test-suite](https://github.com/socketio/engine.io-protocol/tree/main/test-suite)
against `cmd/compliance-server`.

## Why a rewrite

The Go ecosystem had no maintained Socket.IO v4 server: `googollee/go-socket.io`
stopped at protocol v2, and the small v4 implementations are websocket-only,
which breaks the polling-first default of real clients. This project ports
the *behaviour* described by the specs (not the TypeScript code) and keeps
the official suites as the acceptance gates. `docs` on the reasoning live in
[PLAN.md](PLAN.md).

## License

MIT — see [LICENSE](LICENSE).
