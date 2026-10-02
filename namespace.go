package socketio

import (
	"sort"
	"sync"
)

// EventHandler receives one event. ack is nil unless the client requested
// an acknowledgement; calling it replies once. Handlers run serially per
// client, in packet order, and may call back into the socket freely.
type EventHandler func(s *Socket, args []any, ack func(response ...any))

// MiddlewareFunc inspects a socket while its CONNECT packet is being
// processed, before the namespace accepts it. Returning a non-nil error
// refuses the connection: the client receives a CONNECT_ERROR carrying the
// error's message, OnConnect never fires, and the socket is never
// registered. Typical use is checking Socket.Handshake() for credentials.
type MiddlewareFunc func(s *Socket) error

// Namespace is a socket.io namespace: an independent connection space with
// its own sockets, rooms and handlers. Get them from Server.Of.
type Namespace struct {
	srv  *Server
	name string // "/" or "/admin" style

	mu      sync.RWMutex
	adapter Adapter

	onConnect    func(*Socket)
	onDisconnect func(*Socket, string)
	events       map[string]EventHandler
	middlewares  []MiddlewareFunc
}

func newNamespace(srv *Server, name string) *Namespace {
	return &Namespace{
		srv:     srv,
		name:    name,
		adapter: NewInMemoryAdapter(),
		events:  make(map[string]EventHandler),
	}
}

// Name is the namespace's identifier: "/" for the default one.
func (ns *Namespace) Name() string { return ns.name }

// SetAdapter replaces the membership adapter — for example the package's
// in-memory one with the redisadapter's. Do it before serving; swapping
// while clients are connected drops their room memberships.
func (ns *Namespace) SetAdapter(a Adapter) {
	ns.mu.Lock()
	ns.adapter = a
	ns.mu.Unlock()
}

// adapterOf snapshots the current adapter.
func (ns *Namespace) adapterOf() Adapter {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.adapter
}

// Use registers a connection middleware. Middlewares run in registration
// order for every incoming CONNECT, before the connection is accepted;
// register them before serving.
func (ns *Namespace) Use(fn MiddlewareFunc) {
	ns.mu.Lock()
	ns.middlewares = append(ns.middlewares, fn)
	ns.mu.Unlock()
}

// runMiddlewares executes the chain; the first error refuses the socket.
func (ns *Namespace) runMiddlewares(s *Socket) error {
	ns.mu.RLock()
	fns := ns.middlewares
	ns.mu.RUnlock()
	for _, fn := range fns {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}

// OnConnect registers a connection handler.
func (ns *Namespace) OnConnect(fn func(*Socket)) {
	ns.mu.Lock()
	ns.onConnect = fn
	ns.mu.Unlock()
}

// OnDisconnect registers a handler for the socket's last breath. The reason
// strings follow socket.io's vocabulary: "io client disconnect",
// "io server disconnect", "transport close", "ping timeout", "parse error".
func (ns *Namespace) OnDisconnect(fn func(s *Socket, reason string)) {
	ns.mu.Lock()
	ns.onDisconnect = fn
	ns.mu.Unlock()
}

// OnEvent registers a handler for the named event. Registering twice
// replaces the previous handler.
func (ns *Namespace) OnEvent(event string, fn EventHandler) {
	ns.mu.Lock()
	ns.events[event] = fn
	ns.mu.Unlock()
}

// Sockets lists the currently connected socket ids (local sockets only when
// a cross-process adapter is in place).
func (ns *Namespace) Sockets() []string {
	sockets := ns.adapterOf().All()
	ids := make([]string, 0, len(sockets))
	for _, s := range sockets {
		ids = append(ids, s.ID())
	}
	sort.Strings(ids)
	return ids
}

// FetchSockets returns the connected sockets (local only under a
// cross-process adapter).
func (ns *Namespace) FetchSockets() []*Socket {
	return ns.adapterOf().All()
}

// Emit sends the event to every connected socket of the namespace.
func (ns *Namespace) Emit(event string, args ...any) {
	ns.adapterOf().Broadcast(nil, event, args, nil, false)
}

// Volatile returns a namespace-wide target whose emits may be dropped for
// sockets that cannot receive right now. See Socket.Volatile.
func (ns *Namespace) Volatile() *BroadcastTarget {
	return &BroadcastTarget{ns: ns, volatile: true}
}

// To — alias of In — targets a room: only sockets that joined it receive
// what the returned target emits.
func (ns *Namespace) To(room string) *BroadcastTarget {
	return &BroadcastTarget{ns: ns, rooms: []string{room}}
}

// In targets a room; identical to To.
func (ns *Namespace) In(room string) *BroadcastTarget { return ns.To(room) }

func (ns *Namespace) target(rooms []string, except map[string]struct{}) *BroadcastTarget {
	return &BroadcastTarget{ns: ns, rooms: rooms, except: except}
}

func (ns *Namespace) addSocket(s *Socket) {
	ns.adapterOf().AddSocket(s)
}

func (ns *Namespace) removeSocket(s *Socket) {
	ns.adapterOf().RemoveSocket(s)
}

func (ns *Namespace) fireConnect(s *Socket) {
	ns.mu.RLock()
	fn := ns.onConnect
	ns.mu.RUnlock()
	if fn != nil {
		fn(s)
	}
}

func (ns *Namespace) fireDisconnect(s *Socket, reason string) {
	ns.mu.RLock()
	fn := ns.onDisconnect
	ns.mu.RUnlock()
	if fn != nil {
		fn(s, reason)
	}
}

func (ns *Namespace) fireEvent(s *Socket, event string, args []any, ack func(response ...any)) {
	ns.mu.RLock()
	fn := ns.events[event]
	ns.mu.RUnlock()
	if fn == nil {
		// An unhandled acked event still needs an answer, or the client
		// waits out its timeout for nothing.
		if ack != nil {
			ack()
		}
		return
	}
	fn(s, args, ack)
}
