package socketio

import (
	"sort"
	"sync"
)

// Adapter owns a namespace's rooms and membership. The in-memory adapter is
// the default and matches single-process socket.io; an adapter such as the
// one in the redisadapter package relays broadcasts across processes.
//
// All methods may be called from any goroutine. "Local" in the docs means
// this process's sockets only — cross-process membership is the adapter's
// own business.
type Adapter interface {
	// AddSocket registers a freshly accepted socket.
	AddSocket(s *Socket)
	// RemoveSocket forgets the socket and every room it was in.
	RemoveSocket(s *Socket)
	// Add puts the socket in a room.
	Add(s *Socket, room string)
	// Del takes the socket out of a room.
	Del(s *Socket, room string)
	// All lists the local sockets of the namespace.
	All() []*Socket
	// Members lists the local sockets in any of the rooms; nil rooms means
	// every local socket of the namespace.
	Members(rooms []string) []*Socket
	// SocketRooms lists the rooms the local socket is in.
	SocketRooms(s *Socket) []string
	// Broadcast delivers the event to matching local sockets. The redis
	// adapter publishes here and applies on subscription instead.
	Broadcast(rooms []string, event string, args []any, except map[string]struct{}, volatile bool)
}

// DeliverLocal sends an event to recipients, honouring the except list and
// the volatile drop rule. Adapters use it as their apply step.
func DeliverLocal(recipients []*Socket, except map[string]struct{}, volatile bool, event string, args []any) {
	for _, s := range recipients {
		if except != nil {
			if _, skip := except[s.ID()]; skip {
				continue
			}
		}
		if volatile {
			_ = s.Volatile().Emit(event, args...)
			continue
		}
		_ = s.Emit(event, args...)
	}
}

// InMemoryAdapter is the default adapter: rooms and membership live in
// this process and vanish with it.
type InMemoryAdapter struct {
	mu      sync.RWMutex
	sockets map[string]*Socket
	rooms   map[string]map[*Socket]struct{}
}

// NewInMemoryAdapter creates the default adapter for a namespace.
func NewInMemoryAdapter() *InMemoryAdapter {
	return &InMemoryAdapter{
		sockets: make(map[string]*Socket),
		rooms:   make(map[string]map[*Socket]struct{}),
	}
}

func (a *InMemoryAdapter) AddSocket(s *Socket) {
	a.mu.Lock()
	a.sockets[s.ID()] = s
	a.mu.Unlock()
}

func (a *InMemoryAdapter) RemoveSocket(s *Socket) {
	a.mu.Lock()
	delete(a.sockets, s.ID())
	for room, members := range a.rooms {
		delete(members, s)
		if len(members) == 0 {
			delete(a.rooms, room)
		}
	}
	a.mu.Unlock()
}

func (a *InMemoryAdapter) Add(s *Socket, room string) {
	a.mu.Lock()
	set := a.rooms[room]
	if set == nil {
		set = make(map[*Socket]struct{})
		a.rooms[room] = set
	}
	set[s] = struct{}{}
	a.mu.Unlock()
}

func (a *InMemoryAdapter) Del(s *Socket, room string) {
	a.mu.Lock()
	if set := a.rooms[room]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(a.rooms, room)
		}
	}
	a.mu.Unlock()
}

func (a *InMemoryAdapter) All() []*Socket {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*Socket, 0, len(a.sockets))
	for _, s := range a.sockets {
		out = append(out, s)
	}
	return out
}

func (a *InMemoryAdapter) Members(rooms []string) []*Socket {
	a.mu.RLock()
	defer a.mu.RUnlock()
	seen := make(map[*Socket]struct{})
	var out []*Socket
	if len(rooms) == 0 {
		for _, s := range a.sockets {
			out = append(out, s)
		}
		return out
	}
	for _, room := range rooms {
		for s := range a.rooms[room] {
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func (a *InMemoryAdapter) SocketRooms(s *Socket) []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var rooms []string
	for room, members := range a.rooms {
		if _, ok := members[s]; ok {
			rooms = append(rooms, room)
		}
	}
	sort.Strings(rooms)
	return rooms
}

func (a *InMemoryAdapter) Broadcast(rooms []string, event string, args []any, except map[string]struct{}, volatile bool) {
	DeliverLocal(a.Members(rooms), except, volatile, event, args)
}
