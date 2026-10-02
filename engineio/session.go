package engineio

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// CloseReason tells the application which side ended a session and why.
// The socket.io layer maps these onto its own disconnect reasons.
type CloseReason string

const (
	// CloseTransport means the client asked to close (close packet or a
	// dropped transport).
	CloseTransport CloseReason = "transport close"
	// CloseTimeout means nothing arrived within PingInterval+PingTimeout.
	CloseTimeout CloseReason = "ping timeout"
	// CloseProtocol means the peer spoke something the v4 protocol does not
	// allow; the session is ended rather than guessing.
	CloseProtocol CloseReason = "protocol error"
	// CloseServer means the server is shutting down.
	CloseServer CloseReason = "server close"
)

// Session is one Engine.IO connection: an id, a transport that may switch
// from polling to WebSocket, and the heartbeat keeping it alive.
//
// A Session is safe for concurrent use. Send/SendText/SendBinary may be
// called from any goroutine; callbacks run on the session's dispatch
// goroutine, one at a time.
type Session struct {
	id  string
	srv *Server

	mu        sync.Mutex
	buf       []packet.Packet // outbound, waiting for the next parked poll
	wake      chan struct{}   // releases the parked poll when buf grows
	done      chan struct{}   // closed once, on session end
	closed    bool
	reason    CloseReason
	polling   bool   // a long-poll GET is parked for this session
	flushed   bool   // the transport switched away from polling; a parked poll retires with a noop
	transport string // "polling" or "websocket"; a wsConn implies "websocket"
	ws        wsConn // non-nil once the WebSocket transport took over
	hbTimer   *time.Timer
	hbArmed   time.Time // when the live timer was scheduled
	lastRecv  time.Time

	inQ    []inbound // received Message packets waiting for the app
	inCond *sync.Cond
}

// inbound is a Message packet handed to the application.
type inbound struct {
	data   []byte
	binary bool
}

// wsConn is the slice of the WebSocket transport a session needs, so the
// session layer does not depend on gorilla types (M3 fills it in).
type wsConn interface {
	writePacket(p packet.Packet) error
	close() error
}

func newSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is an unrecoverable runtime fault; a
		// predictable id would let sessions hijack each other.
		panic("engineio: cannot read random bytes for session id: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func newSession(srv *Server) *Session {
	s := &Session{
		id:        newSessionID(),
		srv:       srv,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		transport: "polling",
		lastRecv:  time.Now(),
	}
	s.inCond = sync.NewCond(&s.mu)
	return s
}

// ID is the Engine.IO session id the client passes back on every request.
func (s *Session) ID() string { return s.id }

// Done is closed when the session ends, for whichever reason.
func (s *Session) Done() <-chan struct{} { return s.done }

// Send queues any packet to the client. The socket.io layer uses it to push
// its framed packets through the Message channel.
func (s *Session) Send(p packet.Packet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendLocked(p)
}

// sendLocked is Send for callers already holding the session mutex — the
// packet handlers run under it, and a non-reentrant lock means the public
// Send cannot be called from there.
func (s *Session) sendLocked(p packet.Packet) error {
	if s.closed {
		return ErrClosed
	}
	if size := 1 + len(p.Data); size > s.srv.opts.MaxPayload {
		s.closeLocked(CloseProtocol)
		return ErrPayloadTooLarge
	}
	if s.ws != nil {
		return s.ws.writePacket(p)
	}
	s.buf = append(s.buf, p)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// SendText sends a text Message packet.
func (s *Session) SendText(data string) error {
	return s.Send(packet.Packet{Type: packet.Message, Data: []byte(data)})
}

// SendBinary sends a binary Message packet. Over polling it travels as
// "b"+base64; over WebSocket as a binary frame.
func (s *Session) SendBinary(data []byte) error {
	return s.Send(packet.Packet{Type: packet.Message, Data: data, Binary: true})
}

// Close ends the session and notifies the client. Safe to call twice; only
// the first call has an effect.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(CloseTransport)
}

func (s *Session) closeLocked(reason CloseReason) {
	if s.closed {
		return
	}
	s.closed = true
	s.reason = reason
	if s.hbTimer != nil {
		s.hbTimer.Stop()
	}
	s.srv.removeSession(s)
	// Tell the client before tearing down so a parked poll carries the
	// close packet instead of dying with the connection.
	s.buf = append(s.buf, packet.Packet{Type: packet.Close})
	if s.ws != nil {
		_ = s.ws.close()
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	close(s.done)
	// Waking the dispatcher is what retires it, and it is where OnClose
	// fires; without this a closed session leaks the goroutine.
	s.inCond.Broadcast()
}

func (s *Session) reasonLocked() CloseReason {
	return s.reason
}

// handleIncoming processes one decoded packet from the client. Called with
// the session mutex held by the polling/websocket handlers.
func (s *Session) handleIncomingLocked(p packet.Packet) {
	s.lastRecv = time.Now()
	s.armHeartbeatLocked()
	switch p.Type {
	case packet.Ping:
		_ = s.sendLocked(packet.Packet{Type: packet.Pong, Data: p.Data})
	case packet.Message:
		// Queueing never blocks (see dispatch), so packet handlers never
		// sit on the mutex waiting for the application to keep up.
		s.inQ = append(s.inQ, inbound{data: p.Data, binary: p.Binary})
		s.inCond.Signal()
	case packet.Close:
		s.closeLocked(CloseTransport)
	case packet.Upgrade, packet.Noop:
		// Upgrade belongs on the new transport; a stray one on the old
		// transport is harmless. Noop never travels client→server, but
		// ignoring beats killing sessions over it.
	case packet.Open, packet.Pong:
		// Only the server sends these; a client speaking them is broken.
		s.closeLocked(CloseProtocol)
	}
}

func (s *Session) deadline() time.Duration {
	return s.srv.opts.PingInterval + s.srv.opts.PingTimeout
}

// armHeartbeatLocked schedules the expiry check. Every received packet
// re-arms, so the live timer always measures from the last traffic; the
// hbArmed stamp tells a late-fired stale timer to stand down.
func (s *Session) armHeartbeatLocked() {
	s.hbArmed = time.Now()
	if s.hbTimer != nil {
		s.hbTimer.Stop()
	}
	s.hbTimer = time.AfterFunc(s.deadline(), s.checkHeartbeat)
}

func (s *Session) checkHeartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.lastRecv.After(s.hbArmed) {
		// Traffic arrived after this timer was scheduled; the re-arm that
		// traffic triggered owns the deadline now.
		return
	}
	s.closeLocked(CloseTimeout)
}

// dispatch runs for the session's lifetime, serialising callbacks so the
// application never sees messages out of order. Messages are delivered with
// the mutex released: a handler that echoes back through Send would
// otherwise deadlock against the packet handler holding the lock on a full
// queue.
func (s *Session) dispatch() {
	s.mu.Lock()
	for {
		for len(s.inQ) == 0 && !s.closed {
			s.inCond.Wait()
		}
		if len(s.inQ) == 0 {
			break // closed and fully drained
		}
		msg := s.inQ[0]
		s.inQ = s.inQ[1:]
		s.mu.Unlock()
		if fn := s.srv.OnMessage; fn != nil {
			fn(s, msg.data, msg.binary)
		} else {
			s.srv.warnNoMessageHandler()
		}
		s.mu.Lock()
	}
	reason := s.reason
	s.mu.Unlock()
	if fn := s.srv.OnClose; fn != nil {
		fn(s, reason)
	}
}

// takeBufLocked hands the pending outbound packets to a poll.
func (s *Session) takeBufLocked() []packet.Packet {
	pkts := s.buf
	s.buf = nil
	return pkts
}
