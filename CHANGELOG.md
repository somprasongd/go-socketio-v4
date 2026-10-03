# Changelog

## v0.3.1 — 2026-10-03

Fixes all 18 findings from the first full review of `main`.

### Transport and protocol

- Reject empty Engine.IO text packets without panicking, enforce `MaxPayload`
  on inbound WebSocket messages, and validate binary packet headers before
  buffering attachments. Binary packets now allow at most 10 attachments.
- Add `engineio.Options.WriteTimeout` (default: 5 seconds). Failed WebSocket
  writes close the session instead of indefinitely blocking delivery or shutdown.
- Keep server Ping scheduling independent of application traffic. Only Pong
  acknowledges an outstanding heartbeat; missing Pong closes the session.
- Register sessions synchronously before dispatching their first message, close
  the underlying transport on Socket.IO parse errors, and restore polling after
  a failed WebSocket upgrade probe. Pending probes have bounded lifetimes.
- Return CORS headers for allowed origins and handle OPTIONS preflights. Add
  `engineio.Options.AllowCredentials` for credentialed browser requests.

### Acknowledgements and recovery

- Complete acknowledgement waiters exactly once when send failure races with
  disconnect; successful acknowledgements now close their result channel.
- Match acknowledgement IDs to the originating namespace and socket, cancel
  that socket's waiters on namespace disconnect, and ignore late acknowledgements.
- Add `Socket.EmitWithAckContext` for caller cancellation and deadlines.
  `EmitWithAck` now has a 30-second default timeout; expiration removes the waiter
  and closes its channel empty.
- Make `Disconnect` on an obsolete recovered socket handle a no-op, preserving
  the active recovered connection and its room membership.

### Binary encoding and verification

- Preserve binary attachments inside typed Go maps, slices, arrays, pointers and
  structs, including Redis Pub/Sub relay. Keep JSON field visibility, tags,
  integer precision and custom marshalers; reject nesting beyond 32 levels.
- Make Redis broadcast exclusion tests observe the full requested time window,
  including forbidden frames that arrive after unrelated traffic.
- Enable pipeline failure propagation in the CI race-test step and reject every
  failed Go test/package, in addition to requiring real-client and Redis
  acceptance tests to pass. Add regression tests for the acceptance gate.
- Add transport, acknowledgement, recovery and binary regression tests, plus
  real JavaScript client checks for typed binary payloads and busy heartbeats.

Validation before release: 215 Go tests passed with the race detector and an
owned disposable Redis 7.2.16 instance, including the three-process distributed
recovery and Redis disconnect/resume tests; JavaScript interop 19/19; official
Engine.IO compliance 24/24; build, vet, formatting, dependency verification and
four CI-gate unit tests passed. GitHub Actions also gates publication.

## v0.3.0 — 2026-10-03

- Add the opt-in Redis Streams adapter for cross-instance connection-state
  recovery, durable event offsets, persisted sessions and replay.
- Add distributed JavaScript acceptance tests and real Redis failure/resume
  coverage.

## v0.2.0

- Add namespace middleware and CONNECT authentication, an origin allowlist,
  volatile emits, the adapter interface and Redis Pub/Sub broadcast relay.
- Add local connection-state recovery with restored IDs, rooms, data and missed
  events.

## v0.1.0

- Initial Socket.IO v4 and Engine.IO v4 server: polling and WebSocket transports,
  upgrades, server heartbeat, namespaces, rooms, broadcasts, acknowledgements
  and binary attachments.
