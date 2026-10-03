# go-socketio-v4

A Socket.IO v4 server for Go, written from the official protocol
specifications and validated against them — the official Engine.IO
compliance suite (24/24 passing) and the real `socket.io-client` v4
JavaScript client (19/19 interop scenarios passing).

## Install

```sh
go get github.com/somprasongd/go-socketio-v4
```

The transport uses `gorilla/websocket`; the Redis adapters use `go-redis/v9`.

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

Acked emits from the server can use a caller deadline:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
res, ok := <-s.EmitWithAckContext(ctx, "ask-for-config")
if ok {
    // the client answered; res holds the ack arguments
}
```

`EmitWithAck` remains available with a 30-second default timeout. Both APIs
close their channel after one result, or empty on timeout, cancellation, send
failure or namespace/transport disconnect. Acks are isolated by namespace.

Binary values travel as protocol attachments automatically — pass `[]byte`
inside typed maps, slices, arrays, pointers and structs (JSON tags and custom
marshalers are preserved), up to 32 nested levels. Packets support at most 10
binary attachments; excessive or malformed inbound headers close the connection:

```go
s.Emit("photo", map[string]any{"name": "x", "data": []byte{0x01, 0x02}})
```

Other namespaces:

```go
admin := srv.Of("/admin")
admin.OnConnect(func(s *socketio.Socket) { s.Emit("welcome", "admin here") })
```

Connection middleware enforces credentials before the namespace accepts a
client — the CONNECT auth payload (`io(url, { auth: { token } })`) arrives
as `Socket.Handshake()`, and a middleware error becomes a CONNECT_ERROR:

```go
ns.Use(func(s *socketio.Socket) error {
	if s.Handshake()["token"] != expected {
		return errors.New("invalid credentials")
	}
	return nil
})
```

Cross-origin browser polling uses the same `AllowedOrigins` policy as WebSocket.
Allowed origins receive CORS response headers and OPTIONS preflights are handled.
Set `engineio.Options.AllowCredentials` when credentialed requests are required.
The default still allows every origin; set an explicit allowlist for a restricted
endpoint. `MaxPayload` applies to inbound WebSocket messages as well as polling;
`WriteTimeout` bounds WebSocket writes and defaults to 5 seconds. A failed write
closes the transport. Only Pong acknowledges a server heartbeat; application
traffic never postpones Ping.

Connection-state recovery keeps a session across an unexpected drop:

```go
ns.EnableRecovery(nil) // or &socketio.RecoveryOptions{...}
ns.OnConnect(func(s *socketio.Socket) {
	if s.Recovered() {
		log.Println("welcome back", s.ID()) // same id, rooms and data
	}
})
```

Running several processes behind a load balancer, relay broadcasts through
Redis (membership stays local per process):

```go
import "github.com/somprasongd/go-socketio-v4/redisadapter"

ns.SetAdapter(redisadapter.New("/", rdb, "my-app-relay"))
```

### Recovery with multiple processes

The classic `redisadapter` uses Pub/Sub. Incoming broadcasts buffer events for
held sessions on each receiving instance, but recovery still requires returning
to the same process. Pub/Sub messages lost while an instance is disconnected
from Redis cannot be replayed. Sticky routing alone does not fix that loss.

Use the opt-in Streams adapter for recovery across instances (Redis 7.2+):

```go
import (
    "context"
    "log"

    "github.com/redis/go-redis/v9"
    "github.com/somprasongd/go-socketio-v4/redisstreamsadapter"
)

rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    ContextTimeoutEnabled: true,
})
ad, err := redisstreamsadapter.New(context.Background(), "/", rdb,
    &redisstreamsadapter.Options{
        Prefix: "my-app", // same on every instance of this deployment
        OnError: func(err error) { log.Print(err) },
    })
if err != nil {
    log.Fatal(err)
}
defer ad.Close() // Redis client lifecycle remains the caller's responsibility
ns.SetAdapter(ad)
ns.EnableRecovery(nil)
```

Configure every instance with the same deployment prefix, codec, retention and
recovery limits. Every instance reads its namespace stream independently. Durable events receive
a shared stream offset; disconnected sessions live in Redis with a TTL. A
socket automatically joins its own ID room, so `ns.To(id).Volatile().Emit(...)`
also targets an individual client across instances. Apps control room admission.

Volatile events use an ephemeral channel and are never stored for recovery.
Events with acknowledgements also carry no recovery offset and are not replayed.
Direct acked emits remain local to the socket handle's process. Redis append
failure stops delivery of that durable event: direct `Emit` returns an error,
while namespace/room broadcasts report through `OnError`. That callback must
return promptly and must not call back into the namespace, since delivery may
hold its lock. Set `ContextTimeoutEnabled: true` on the supplied Redis client
so operation deadlines also bound network I/O.

`Options` defaults: stream length 100,000, read batch 100, read block 1 second,
operation timeout 5 seconds. Block time must be shorter than operation timeout.
Retention is an event count limit; tune it for your event rate and recovery
window. Namespace keys are isolated by deployment prefix and namespace, with
Redis hash tags keeping atomic session operations in one hash slot.

`Socket.Data` uses JSON by default: a restored struct becomes a JSON object,
and numbers use JSON decoding semantics. Supply `Options.DataCodec` with
concurrency-safe `Encode(any) ([]byte, error)` and `Decode([]byte) (any, error)`
methods to preserve application types. Encoding failure leaves the session
unrecoverable and reports through `OnError`.

Recovery validates a retained offset actually issued to the session. Expired
sessions, trimmed history, concurrent claims, and replay exceeding
`MaxBufferedEvents` start a fresh session (`Recovered() == false`). Keep an
application resynchronization path for this case. Backend operational failures
return a retryable CONNECT_ERROR instead of silently creating a fresh session.
Middleware rejection returns only CONNECT_ERROR; CONNECT, replay, then
`OnConnect` precede queued live delivery on successful restore.

Persisted disconnected sessions survive a Go process restart. A process crash
before the disconnect snapshot is saved is not covered. Redis durability and
availability depend on its deployment configuration. The Redis data format is
private to this Go library and does not interoperate with the JavaScript Redis
adapter or emitter. HTTP polling still needs sticky routing for each live
Engine.IO session, even when Socket.IO reconnect recovery is distributed.

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
| Socket.IO v4 | volatile emits (drop instead of buffer when the client cannot receive) | ✅ |
| Socket.IO v4 | connection middleware (`ns.Use`) with the CONNECT auth payload | ✅ |
| Socket.IO v4 | connection-state recovery (pid/offset, id+rooms+data restored, replay) | ✅ |
| Socket.IO v4 | cross-process broadcasting (`redisadapter`) | ✅ |
| Socket.IO v4 | volatile over Redis Pub/Sub relay (including socket-ID rooms) | ✅ |
| Socket.IO v4 | local recovery with Redis Pub/Sub (same instance, while subscribed) | ✅ |
| Socket.IO v4 | cross-instance recovery with `redisstreamsadapter` | ✅ |

Disconnect reasons reach `OnDisconnect` in socket.io's vocabulary:
`io client disconnect`, `io server disconnect`, `transport close`,
`ping timeout`, `parse error`, `server shutting down`.

## Testing

```sh
go test ./...            # unit + golden protocol tests + e2e + JS interop
go test -race ./...      # the concurrency suites
make compliance          # the official engine.io-protocol test suite (24/24)
SOCKETIO_REDIS_ADDR=127.0.0.1:6379 make integration # owned disposable Redis 7.2+
```

The interop tests need `node` (v18+) with `socket.io-client`; they skip with
an explicit reason when it is missing. `compliance/run.sh` runs the suite
copied verbatim from
[engine.io-protocol/test-suite](https://github.com/socketio/engine.io-protocol/tree/main/test-suite)
against `cmd/compliance-server`.


The distributed acceptance suite requires Node with `interop/node_modules`
installed (`cd interop && npm ci`) and a disposable Redis endpoint. It launches
three separate Go server processes and the real JavaScript client, checks both
namespaces and binary replay, restarts the original process after a persisted
disconnect, and interrupts one adapter's Redis connections through a local TCP
proxy to verify stream resume and failed-append behavior. Without the environment
variable, these integration tests explicitly skip; unit tests use miniredis.
Tests use isolated prefixes and session TTLs; use an owned disposable instance,
not a production endpoint.

GitHub Actions runs these checks on pushes to `main`, version tags, pull
requests, and manual dispatch: build/vet/format/dependency verification;
all Go tests with the race detector and a Redis 7.2 service; and official
Engine.IO protocol compliance. The test job installs the JavaScript client
and requires the interop and real Redis acceptance tests to pass explicitly,
so missing dependencies, skipped integration tests, unrelated test failures and
race failures cannot produce a green CI. See [CHANGELOG.md](CHANGELOG.md) for releases.

## Why a rewrite

The Go ecosystem had no maintained Socket.IO v4 server: `googollee/go-socket.io`
stopped at protocol v2, and the small v4 implementations are websocket-only,
which breaks the polling-first default of real clients. This project ports
the *behaviour* described by the specs (not the TypeScript code) and keeps
the official suites as the acceptance gates. The reasoning and the
milestone-by-milestone record live in [PLAN.md](PLAN.md).

## License

MIT — see [LICENSE](LICENSE).
