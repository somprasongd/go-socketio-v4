package socketio

import (
	"encoding/hex"
	"log"
	"sync"

	"crypto/rand"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// client is one Socket.IO client: everything riding a single engine.io
// session — its namespace connections and the ack bookkeeping.
//
// Two locks, always taken in this order: mu guards the inbound packet state
// machine (so packets stay ordered), sendMu guards the wire sequence (a
// BINARY_EVENT's attachments must directly follow its text packet). User
// handlers run under mu, and their Sends take sendMu — never the reverse —
// which is what keeps handler echoes deadlock-free.
type client struct {
	srv  *Server
	sess sendSink

	mu       sync.Mutex
	conns    map[string]*Socket // by namespace name
	needBin  int
	binText  string
	bins     [][]byte
	closed   bool
	reason   string
	dispatch *sync.Cond

	sendMu sync.Mutex

	ackMu    sync.Mutex
	ackNext  int64
	ackWaits map[int64]chan []any
}

func newClient(srv *Server, sess sendSink) *client {
	c := &client{
		srv:      srv,
		sess:     sess,
		conns:    make(map[string]*Socket),
		ackWaits: make(map[int64]chan []any),
	}
	c.dispatch = sync.NewCond(&c.mu)
	return c
}

// onMessage runs one inbound packet through the state machine. Binary
// Engine.IO packets only make sense as attachments of a binary text packet,
// so the client buffers until the declared count arrives.
func (c *client) onMessage(data []byte, isBinary bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.needBin > 0 {
		if !isBinary {
			c.killLocked(reasonParseError)
			return
		}
		c.bins = append(c.bins, data)
		if len(c.bins) < c.needBin {
			c.mu.Unlock()
			return
		}
		text, bins := c.binText, c.bins
		c.needBin, c.binText, c.bins = 0, "", nil
		c.handlePacketLocked(text, bins)
		c.mu.Unlock()
		return
	}
	if isBinary {
		// Attachments never travel without their text packet in front.
		c.killLocked(reasonParseError)
		return
	}
	if n := parser.NeededAttachments(string(data)); n > 0 {
		c.needBin, c.binText, c.bins = n, string(data), nil
		c.mu.Unlock()
		return
	}
	c.handlePacketLocked(string(data), nil)
	c.mu.Unlock()
}

// handlePacketLocked decodes and dispatches one packet. Decode failures end
// the client with a parse error, mirroring the JS server.
func (c *client) handlePacketLocked(text string, bins [][]byte) {
	pkt, err := parser.Decode(text, bins)
	if err != nil {
		log.Printf("socketio: dropping client %s: %v", c.sess.ID(), err)
		c.killLocked(reasonParseError)
		return
	}

	name := pkt.Namespace
	if name == "" {
		name = "/"
	}

	switch pkt.Type {
	case parser.Connect:
		c.handleConnectLocked(name)

	case parser.Disconnect:
		if s := c.conns[name]; s != nil {
			c.removeConnLocked(s)
			s.ns.fireDisconnect(s, reasonClientDisconnect)
		}

	case parser.Event, parser.BinaryEvent:
		c.handleEventLocked(name, pkt)

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

// handleConnectLocked opens a namespace connection. Unknown namespaces are
// refused with CONNECT_ERROR, as the spec requires.
func (c *client) handleConnectLocked(name string) {
	if c.conns[name] != nil {
		return // already connected; ignore the repeat
	}
	c.srv.mu.Lock()
	ns := c.srv.namespaceLocked(name)
	c.srv.mu.Unlock()
	if ns == nil {
		c.sendPacket(parser.Packet{
			Type:      parser.ConnectError,
			Namespace: nsName(name),
			Data:      map[string]any{"message": "Invalid namespace"},
		})
		return
	}
	s := &Socket{
		id: newSocketID(),
		ns: ns,
		c:  c,
	}
	c.conns[name] = s
	ns.addSocket(s)
	c.sendPacket(parser.Packet{
		Type:      parser.Connect,
		Namespace: nsName(name),
		Data:      map[string]any{"sid": s.id},
	})
	ns.fireConnect(s)
}

func (c *client) handleEventLocked(name string, pkt parser.Packet) {
	s := c.conns[name]
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
			// A late ack after the client is gone is dropped by SendPacket.
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

// removeConnLocked drops the namespace connection from the client.
func (c *client) removeConnLocked(s *Socket) {
	delete(c.conns, s.ns.name)
	s.ns.removeSocket(s)
}

// kill ends the client after a transport-level close, notifying every
// namespace connection and releasing pending acks.
func (c *client) kill(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.killLocked(reason)
}

func (c *client) killLocked(reason string) {
	if c.closed {
		return
	}
	c.closed = true
	c.reason = reason
	conns := make([]*Socket, 0, len(c.conns))
	for _, s := range c.conns {
		conns = append(conns, s)
	}
	c.conns = map[string]*Socket{}
	for _, s := range conns {
		s.ns.removeSocket(s)
		s.ns.fireDisconnect(s, reason)
	}
	c.dispatch.Broadcast()

	c.ackMu.Lock()
	waits := c.ackWaits
	c.ackWaits = map[int64]chan []any{}
	c.ackMu.Unlock()
	for _, wait := range waits {
		close(wait) // closed empty: the caller's wait sees a timeout
	}

	c.srv.removeClient(c.sess)
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
