# go-socketio-v4

Socket.IO v4 + Engine.IO v4 server implementation for Go, written from the
official protocol specifications and validated against them.

Work in progress — see [PLAN.md](PLAN.md) for the milestones and current status.

## Status

Not usable yet. The first protocol-level code lands with milestone M1.

## Why

The Go ecosystem has no maintained Socket.IO v4 server: `googollee/go-socket.io`
stopped at protocol v2, and the small v4 implementations are websocket-only,
which breaks the polling-first default of real clients. This project implements
Engine.IO v4 (polling + WebSocket + upgrade) and the Socket.IO v4 parser from
the specs, with the official Engine.IO compliance suite and the real JS client
as the acceptance gates.

## License

MIT
