package socketio

import (
	"strconv"
	"sync"
	"time"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// Socket is one client connection to one namespace.
type Socket struct {
	id        string
	ns        *Namespace
	c         *client
	handshake map[string]any
	rec       *recoveryEntry // nil unless recovery is enabled
	recovered bool

	mu   sync.Mutex
	data any
}

// ID is the namespace-connection id, as also sent to the client in the
// connect handshake.
func (s *Socket) ID() string { return s.id }

// Handshake returns the auth payload the client sent with its CONNECT
// packet — socket.io's `socket.handshake.auth`. Nil when the client sent
// no auth. Read it from connection middleware to enforce credentials.
func (s *Socket) Handshake() map[string]any { return s.handshake }

// Recovered reports whether this connection is a restored session (the
// client came back inside the recovery window and kept its id and rooms).
func (s *Socket) Recovered() bool { return s.recovered }

// Namespace is the namespace the socket belongs to.
func (s *Socket) Namespace() *Namespace { return s.ns }

// SetData stores arbitrary per-connection state (socket.io's socket.data).
// Safe for concurrent use.
func (s *Socket) SetData(v any) {
	s.mu.Lock()
	s.data = v
	s.mu.Unlock()
}

// GetData returns the stored per-connection state.
func (s *Socket) GetData() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

func (s *Socket) getData() any { return s.GetData() }

// Emit sends an event to this socket. Binary values ([]byte) inside args
// are carried as protocol attachments automatically.
func (s *Socket) Emit(event string, args ...any) error {
	return s.sendEvent(parser.Event, event, args, 0, false, false)
}

// Volatile returns a view of the socket whose emits are allowed to be
// dropped: if the client cannot receive right now (no parked poll, or the
// transport just went away) the event vanishes instead of being buffered.
// Use it for data where stale is worse than missing — positions, ticks,
// live counters — never for events the client must not lose.
func (s *Socket) Volatile() *VolatileSocket {
	return &VolatileSocket{s: s}
}

// VolatileSocket emits on a socket with the volatile flag set.
type VolatileSocket struct {
	s *Socket
}

// Emit sends the event, dropping it if the client cannot receive right now.
func (v *VolatileSocket) Emit(event string, args ...any) error {
	return v.s.sendEvent(parser.Event, event, args, 0, false, true)
}

// EmitWithAck sends an event and expects the client to answer. The returned
// channel yields exactly one result — the client's ack arguments — and is
// then closed; if the socket dies or the client never answers, it closes
// empty and the caller's timeout (or ctx) decides. Suggested use:
//
//	select {
//	case res := <-ch: use res
//	case <-time.After(2 * time.Second): give up
//	}
func (s *Socket) EmitWithAck(event string, args ...any) <-chan []any {
	id := s.c.nextAckID()
	wait := s.c.awaitAck(id)
	if err := s.sendEvent(parser.Event, event, args, id, true, false); err != nil {
		s.c.ackMu.Lock()
		delete(s.c.ackWaits, id)
		s.c.ackMu.Unlock()
		close(wait)
	}
	return wait
}

// Join puts the socket in a room. Rooms are per-namespace and vanish when
// empty.
func (s *Socket) Join(room string) {
	s.ns.adapterOf().Add(s, room)
	if s.rec != nil {
		s.rec.trackRoom(room, true)
	}
}

// Leave removes the socket from a room.
func (s *Socket) Leave(room string) {
	s.ns.adapterOf().Del(s, room)
	if s.rec != nil {
		s.rec.trackRoom(room, false)
	}
}

// Rooms lists the rooms the socket is in (local rooms only under a
// cross-process adapter).
func (s *Socket) Rooms() []string {
	return s.ns.adapterOf().SocketRooms(s)
}

// Broadcast targets every socket of the namespace except this one.
func (s *Socket) Broadcast() *BroadcastTarget {
	return s.ns.target(nil, map[string]struct{}{s.ID(): {}})
}

// To targets a room, excluding this socket — the socket.io "rooms I am in,
// minus me" idiom.
func (s *Socket) To(room string) *BroadcastTarget {
	return s.ns.target([]string{room}, map[string]struct{}{s.ID(): {}})
}

// Disconnect closes the namespace connection from the server side. The
// client is told with a disconnect packet and OnDisconnect fires with
// "io server disconnect". Safe to call from inside handlers.
func (s *Socket) Disconnect() {
	c := s.c
	c.mu.Lock()
	if c.conns[s.ns.name] == s {
		delete(c.conns, s.ns.name)
		c.sendPacket(parser.Packet{
			Type:      parser.Disconnect,
			Namespace: nsName(s.ns.name),
		})
	}
	c.mu.Unlock()
	s.ns.removeSocket(s)
	s.finalizeEntry(reasonServerDisconnect)
	s.ns.fireDisconnect(s, reasonServerDisconnect)
}

func (s *Socket) sendEvent(base parser.Type, event string, args []any, ackID int64, hasAck bool, volatile bool) error {
	if volatile && !s.c.sess.Writable() {
		// volatile: the client cannot take it this instant; let it go
		return nil
	}
	full := make([]any, 0, len(args)+2)
	full = append(full, event)
	full = append(full, args...)

	// With recovery enabled every event carries its offset as the last
	// element of the data array — the wire contract the JS client uses to
	// report back what it has processed.
	if s.rec != nil {
		off := s.rec.stamp()
		full = append(full, strconv.FormatInt(off, 10))
		text, bins, err := parser.Encode(parser.Packet{
			Type:      base,
			Namespace: nsName(s.ns.name),
			ID:        ackID,
			HasID:     hasAck,
			Data:      full,
		})
		if err != nil {
			return err
		}
		s.rec.remember(s.rec.maxEvents, recoveryEvent{offset: off, text: text, bins: bins})
		return s.c.sendEncoded(text, bins)
	}

	return s.c.sendPacket(parser.Packet{
		Type:      base,
		Namespace: nsName(s.ns.name),
		ID:        ackID,
		HasID:     hasAck,
		Data:      full,
	})
}

// finalizeEntry decides the fate of the recovery entry at disconnect time:
// unexpected closes hold the session for the window, deliberate disconnects
// drop it (matching socket.io, which never recovers those).
func (s *Socket) finalizeEntry(reason string) {
	if s.rec == nil {
		return
	}
	recoverable := reason == reasonTransportClose || reason == reasonPingTimeout || reason == reasonParseError
	if !recoverable {
		s.ns.recoveryDrop(s.rec.pid)
		return
	}
	s.rec.mu.Lock()
	s.rec.discAt = time.Now()
	s.rec.rooms = s.ns.adapterOf().SocketRooms(s)
	s.rec.data = s.getData()
	s.rec.mu.Unlock()
}

// BroadcastTarget collects recipients for an emit. except holds socket ids.
type BroadcastTarget struct {
	ns       *Namespace
	rooms    []string
	except   map[string]struct{}
	volatile bool
}

// To widens the target with another room.
func (t *BroadcastTarget) To(room string) *BroadcastTarget {
	return &BroadcastTarget{ns: t.ns, rooms: append(append([]string{}, t.rooms...), room), except: t.except, volatile: t.volatile}
}

// Volatile marks the target's emits as droppable: sockets that cannot
// receive right now are skipped instead of having the event buffered.
func (t *BroadcastTarget) Volatile() *BroadcastTarget {
	return &BroadcastTarget{ns: t.ns, rooms: t.rooms, except: t.except, volatile: true}
}

// Emit sends the event to every socket in the target. Under a cross-process
// adapter the delivery happens on every process that holds matching sockets.
func (t *BroadcastTarget) Emit(event string, args ...any) {
	t.ns.broadcast(t.rooms, event, args, t.except, t.volatile)
}
