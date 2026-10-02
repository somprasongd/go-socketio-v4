package engineio

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// wsTransport carries packets over a WebSocket connection: one frame per
// packet, text frames for text packets and binary frames for binary Message
// packets. Writes are serialised because gorilla connections do not tolerate
// concurrent writers.
type wsTransport struct {
	conn *websocket.Conn
	wmu  chan struct{} // writer token: one write at a time
}

func newWSTransport(conn *websocket.Conn) *wsTransport {
	t := &wsTransport{conn: conn, wmu: make(chan struct{}, 1)}
	t.wmu <- struct{}{}
	return t
}

func (t *wsTransport) writePacket(p packet.Packet) error {
	<-t.wmu
	defer func() { t.wmu <- struct{}{} }()
	if p.Binary {
		return t.conn.WriteMessage(websocket.BinaryMessage, p.Data)
	}
	return t.conn.WriteMessage(websocket.TextMessage, []byte(packet.EncodePacket(p)))
}

func (t *wsTransport) close() error {
	return t.conn.Close()
}

// serveWebSocketRequest upgrades the HTTP request and either starts a fresh
// WebSocket-only session (empty sid) or attaches to an existing one as the
// probing transport (the upgrade dance).
func (srv *Server) serveWebSocketRequest(w http.ResponseWriter, r *http.Request, sid string) {
	// engine.io's default is to allow any origin; origin policy is the
	// embedding application's call, and gorilla's default (same-origin
	// only) would silently break every cross-origin client.
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the HTTP error
	}
	t := newWSTransport(conn)

	if sid == "" {
		srv.serveWebSocketOnly(t)
		return
	}
	s := srv.getSession(sid)
	if s == nil {
		_ = t.close()
		return
	}
	go s.wsReadLoop(t, true)
}

// serveWebSocketOnly starts a fresh session directly on WebSocket. Such a
// client never polls, so the OPEN handshake goes out over the socket itself.
func (srv *Server) serveWebSocketOnly(t *wsTransport) {
	s := newSession(srv)
	srv.mu.Lock()
	srv.sessions[s.id] = s
	srv.mu.Unlock()

	s.mu.Lock()
	s.ws = t
	s.transport = "websocket"
	body, err := srv.handshakeBody(s, nil)
	if err != nil {
		s.closeLocked(CloseServer)
		s.mu.Unlock()
		_ = t.close()
		return
	}
	if err := t.writePacket(packet.Packet{Type: packet.Open, Data: body}); err != nil {
		s.closeLocked(CloseServer)
		s.mu.Unlock()
		return
	}
	s.armHeartbeatLocked()
	s.mu.Unlock()

	if fn := srv.OnSession; fn != nil {
		go fn(s)
	}
	go s.dispatch()
	s.wsReadLoop(t, false)
}

// handshakeBody marshals the Open packet's JSON for a session.
func (srv *Server) handshakeBody(s *Session, upgrades []string) ([]byte, error) {
	return json.Marshal(packet.Handshake{
		SID:          s.id,
		Upgrades:     upgrades,
		PingInterval: int(srv.opts.PingInterval / time.Millisecond),
		PingTimeout:  int(srv.opts.PingTimeout / time.Millisecond),
		MaxPayload:   srv.opts.MaxPayload,
	})
}

// wsReadLoop is the WebSocket reader for one session. It starts in the
// probing state for an upgrade (only probe/upgrade/close packets allowed)
// and becomes the session's sole inbound reader once the client accepts
// with an upgrade packet.
func (s *Session) wsReadLoop(t *wsTransport, probing bool) {
	upgraded := !probing
	for {
		mt, data, err := t.conn.ReadMessage()
		if err != nil {
			s.mu.Lock()
			// A failed probe must not kill the session: pre-upgrade the
			// client is still happily polling on the old channel.
			if upgraded && s.ws == t && !s.closed {
				s.closeLocked(CloseTransport)
			}
			s.mu.Unlock()
			return
		}
		p, derr := decodeFrame(mt, data)

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = t.close()
			return
		}
		if derr != nil {
			s.closeLocked(CloseProtocol)
			s.mu.Unlock()
			_ = t.close()
			return
		}
		s.lastRecv = time.Now()
		s.armHeartbeatLocked()

		switch {
		case p.Type == packet.Ping:
			s.mu.Unlock()
			// Answered on this socket directly: before the switch the
			// session does not route anything through it.
			if err := t.writePacket(packet.Packet{Type: packet.Pong, Data: p.Data}); err != nil {
				_ = t.close()
				return
			}
		case p.Type == packet.Close:
			s.closeLocked(CloseTransport)
			s.mu.Unlock()
			_ = t.close()
			return
		case p.Type == packet.Upgrade && !upgraded:
			upgraded = true
			s.switchToWebSocketLocked(t)
			s.mu.Unlock()
		case p.Type == packet.Message && upgraded:
			// Queueing never blocks, so the reader never sits on the mutex
			// waiting for the application to keep up.
			s.inQ = append(s.inQ, inbound{data: p.Data, binary: p.Binary})
			s.inCond.Signal()
			s.mu.Unlock()
		default:
			// Message before the upgrade completed, or a packet that never
			// travels client→server (Open/Pong/Noop/Upgrade-repeat).
			s.closeLocked(CloseProtocol)
			s.mu.Unlock()
			_ = t.close()
			return
		}
	}
}

// switchToWebSocketLocked moves the session onto t. Anything still buffered
// for the old poll rides the new socket first; the parked poll is woken to
// retire with a noop so the client closes its old channel.
func (s *Session) switchToWebSocketLocked(t *wsTransport) {
	s.ws = t
	s.transport = "websocket"
	for _, p := range s.takeBufLocked() {
		_ = t.writePacket(p)
	}
	s.flushed = true
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// decodeFrame turns one WebSocket frame into a packet: text frames carry the
// text form, binary frames are raw Message payloads with no prefix.
func decodeFrame(mt int, data []byte) (packet.Packet, error) {
	if mt == websocket.BinaryMessage {
		return packet.Packet{Type: packet.Message, Data: data, Binary: true}, nil
	}
	return packet.DecodePacket(string(data))
}
