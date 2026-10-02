package socketio

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"sync"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// client is one Socket.IO client: everything riding a single engine.io
// session — its namespace connections and the ack bookkeeping.
//
// Three locks, always taken in this order: handlerMu serialises the whole
// packet pipeline (decode, handlers, replies) so handlers see packets in
// order; mu guards the client state (connections, closed flag, the binary
// attachment buffer); sendMu guards the wire sequence, because a binary
// event's attachments must directly follow its text packet.
//
// User handlers run while handlerMu is held but mu released, so a handler
// may call back into the client (Socket.Disconnect takes mu) without
// deadlocking. Echoing through Send takes only sendMu.
type client struct {
	srv  *Server
	sess sendSink

	handlerMu sync.Mutex

	mu      sync.Mutex
	conns   map[string]*Socket // by namespace name
	needBin int
	binText string
	bins    [][]byte
	closed  bool

	sendMu sync.Mutex

	ackMu    sync.Mutex
	ackNext  int64
	ackWaits map[int64]chan []any
}

func newClient(srv *Server, sess sendSink) *client {
	return &client{
		srv:      srv,
		sess:     sess,
		conns:    make(map[string]*Socket),
		ackWaits: make(map[int64]chan []any),
	}
}

// onMessage runs one inbound packet through the pipeline. Binary engine.io
// packets only make sense as attachments of a binary text packet, so the
// client buffers until the declared count arrives.
func (c *client) onMessage(data []byte, isBinary bool) {
	c.handlerMu.Lock()
	defer c.handlerMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var text string
	var bins [][]byte
	switch {
	case c.needBin > 0:
		if !isBinary {
			c.mu.Unlock()
			c.kill(reasonParseError)
			return
		}
		c.bins = append(c.bins, data)
		if len(c.bins) < c.needBin {
			c.mu.Unlock()
			return
		}
		text, bins = c.binText, c.bins
		c.needBin, c.binText, c.bins = 0, "", nil
	case isBinary:
		// Attachments never travel without their text packet in front.
		c.mu.Unlock()
		c.kill(reasonParseError)
		return
	default:
		if n := parser.NeededAttachments(string(data)); n > 0 {
			c.needBin, c.binText, c.bins = n, string(data), nil
			c.mu.Unlock()
			return
		}
		text = string(data)
	}

	pkt, err := parser.Decode(text, bins)
	if err != nil {
		log.Printf("socketio: dropping client %s: %v", c.sess.ID(), err)
		c.mu.Unlock()
		c.kill(reasonParseError)
		return
	}
	c.mu.Unlock()

	c.route(pkt)
}

// route dispatches one decoded packet. Everything here runs with mu
// released; state mutations grab it only for the length of the mutation.
func (c *client) route(pkt parser.Packet) {
	name := pkt.Namespace
	if name == "" {
		name = "/"
	}

	switch pkt.Type {
	case parser.Connect:
		c.routeConnect(name)
	case parser.Disconnect:
		c.routeDisconnect(name)
	case parser.Event, parser.BinaryEvent:
		c.routeEvent(name, pkt)
	case parser.Ack, parser.BinaryAck:
		if !pkt.HasID {
			return // nothing to match; ignore
		}
		c.ackMu.Lock()
		wait := c.ackWaits[pkt.ID]
		delete(c.ackWaits, pkt.ID)
		c.ackMu.Unlock()
		if wait != nil {
			wait <- pkt.Args()
		}
	}
}

// routeConnect opens a namespace connection. Unknown namespaces are refused
// with CONNECT_ERROR, as the spec requires.
func (c *client) routeConnect(name string) {
	c.mu.Lock()
	if c.closed || c.conns[name] != nil {
		// already connected; ignore the repeat
		c.mu.Unlock()
		return
	}
	c.srv.mu.Lock()
	ns := c.srv.namespaceLocked(name)
	c.srv.mu.Unlock()
	if ns == nil {
		c.mu.Unlock()
		c.sendPacket(parser.Packet{
			Type:      parser.ConnectError,
			Namespace: nsName(name),
			Data:      map[string]any{"message": "Invalid namespace"},
		})
		return
	}
	s := &Socket{id: newSocketID(), ns: ns, c: c}
	c.conns[name] = s
	c.mu.Unlock()

	ns.addSocket(s)
	c.sendPacket(parser.Packet{
		Type:      parser.Connect,
		Namespace: nsName(name),
		Data:      map[string]any{"sid": s.id},
	})
	ns.fireConnect(s)
}

func (c *client) routeDisconnect(name string) {
	c.mu.Lock()
	s := c.conns[name]
	if s == nil {
		c.mu.Unlock()
		return
	}
	delete(c.conns, name)
	c.mu.Unlock()

	s.ns.removeSocket(s)
	s.ns.fireDisconnect(s, reasonClientDisconnect)
}

func (c *client) routeEvent(name string, pkt parser.Packet) {
	c.mu.Lock()
	s := c.conns[name]
	c.mu.Unlock()
	if s == nil {
		return // events for a namespace we never connected; ignore
	}

	args := pkt.Args()
	event, _ := args[0].(string)
	rest := args[1:]

	var ack func(response ...any)
	if pkt.HasID {
		id := pkt.ID
		ack = func(response ...any) {
			// A late ack after the client is gone is dropped by the sink.
			c.sendPacket(parser.Packet{
				Type:      parser.Ack,
				Namespace: nsName(name),
				ID:        id,
				HasID:     true,
				Data:      response,
			})
		}
	}
	s.ns.fireEvent(s, event, rest, ack)
}

// kill ends the client after a transport-level close or a parse error:
// notifying every namespace connection, releasing pending acks, and
// unregistering. Idempotent; safe from any goroutine.
func (c *client) kill(reason string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conns := make([]*Socket, 0, len(c.conns))
	for _, s := range c.conns {
		conns = append(conns, s)
	}
	c.conns = map[string]*Socket{}
	for _, s := range conns {
		s.ns.removeSocket(s)
	}
	c.ackMu.Lock()
	waits := c.ackWaits
	c.ackWaits = map[int64]chan []any{}
	c.ackMu.Unlock()
	c.srv.removeClient(c.sess)
	c.mu.Unlock()

	for _, wait := range waits {
		close(wait) // closed empty: the caller's wait sees a timeout
	}
	for _, s := range conns {
		s.ns.fireDisconnect(s, reason)
	}
}

// sendPacket encodes and transmits one packet, attachments and all, as an
// atomic wire sequence.
func (c *client) sendPacket(pkt parser.Packet) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	text, bins, err := parser.Encode(pkt)
	if err != nil {
		log.Printf("socketio: cannot encode packet for %s: %v", c.sess.ID(), err)
		return err
	}
	if err := c.sess.SendText(text); err != nil {
		return err
	}
	for _, b := range bins {
		if err := c.sess.SendBinary(b); err != nil {
			return err
		}
	}
	return nil
}

// nextAckID reserves an acknowledgement id for an acked emit.
func (c *client) nextAckID() int64 {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	c.ackNext++
	return c.ackNext
}

func (c *client) awaitAck(id int64) chan []any {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	wait := make(chan []any, 1)
	c.ackWaits[id] = wait
	return wait
}

// newSocketID generates a namespace-connection id in socket.io's shape —
// short, opaque, unique per connection.
func newSocketID() string {
	b := make([]byte, 9)
	if _, err := rand.Read(b); err != nil {
		panic("socketio: cannot read random bytes for socket id: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// nsName normalises a dispatch namespace key for the wire (the default
// namespace is omitted entirely).
func nsName(name string) string {
	if name == "/" {
		return ""
	}
	return name
}
