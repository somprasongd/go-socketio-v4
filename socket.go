package socketio

import (
	"sort"
	"sync"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// Socket is one client connection to one namespace.
type Socket struct {
	id        string
	ns        *Namespace
	c         *client
	handshake map[string]any

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
	s.ns.joinRoom(s, room)
}

// Leave removes the socket from a room.
func (s *Socket) Leave(room string) {
	s.ns.leaveRoom(s, room)
}

// Rooms lists the rooms the socket is in.
func (s *Socket) Rooms() []string {
	s.ns.mu.RLock()
	defer s.ns.mu.RUnlock()
	var rooms []string
	for room, members := range s.ns.rooms {
		if _, ok := members[s]; ok {
			rooms = append(rooms, room)
		}
	}
	sort.Strings(rooms)
	return rooms
}

// Broadcast targets every socket of the namespace except this one.
func (s *Socket) Broadcast() *BroadcastTarget {
	return s.ns.target(nil, map[*Socket]struct{}{s: {}})
}

// To targets a room, excluding this socket — the socket.io "rooms I am in,
// minus me" idiom.
func (s *Socket) To(room string) *BroadcastTarget {
	return s.ns.target([]string{room}, map[*Socket]struct{}{s: {}})
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
	s.ns.fireDisconnect(s, reasonServerDisconnect)
}

func (s *Socket) sendEvent(base parser.Type, event string, args []any, ackID int64, hasAck bool, volatile bool) error {
	if volatile && !s.c.sess.Writable() {
		// volatile: the client cannot take it this instant; let it go
		return nil
	}
	full := make([]any, 0, len(args)+1)
	full = append(full, event)
	full = append(full, args...)
	return s.c.sendPacket(parser.Packet{
		Type:      base,
		Namespace: nsName(s.ns.name),
		ID:        ackID,
		HasID:     hasAck,
		Data:      full,
	})
}

// BroadcastTarget collects recipients for an emit.
type BroadcastTarget struct {
	ns       *Namespace
	rooms    []string
	except   map[*Socket]struct{}
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

// Emit sends the event to every socket in the target.
func (t *BroadcastTarget) Emit(event string, args ...any) {
	t.ns.mu.RLock()
	recipients := make(map[*Socket]struct{})
	if len(t.rooms) == 0 {
		for _, s := range t.ns.sockets {
			recipients[s] = struct{}{}
		}
	} else {
		for _, room := range t.rooms {
			for s := range t.ns.rooms[room] {
				recipients[s] = struct{}{}
			}
		}
	}
	t.ns.mu.RUnlock()

	for s := range recipients {
		if _, skip := t.except[s]; skip {
			continue
		}
		if t.volatile {
			_ = s.Volatile().Emit(event, args...)
			continue
		}
		_ = s.Emit(event, args...)
	}
}
