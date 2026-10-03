package engineio

import (
	"errors"
	"log"
	"net/http"
	"sync"

	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// Errors returned by Session sends and the server.
var (
	// ErrClosed is returned when a packet is sent to an ended session.
	ErrClosed = errors.New("engineio: session closed")
	// ErrPayloadTooLarge is returned when a packet exceeds Options.MaxPayload;
	// the session is closed with CloseProtocol at the same time, because the
	// transport can no longer be trusted to frame correctly.
	ErrPayloadTooLarge = errors.New("engineio: payload too large")
)

// Server is the Engine.IO v4 endpoint. It implements http.Handler; mount it
// at any path (conventionally /engine.io/). Construct with NewServer.
type Server struct {
	opts *Options

	// OnSession runs synchronously after the handshake and before dispatch.
	OnSession func(*Session)
	// OnMessage runs for every Message packet, in arrival order, on the
	// session's dispatch goroutine. Each session is serialised
	// independently, so one slow handler never blocks another session.
	OnMessage func(s *Session, data []byte, isBinary bool)
	// OnClose runs once, after the last message, when the session ends.
	OnClose func(s *Session, reason CloseReason)

	mu       sync.Mutex
	sessions map[string]*Session

	noMessageWarn sync.Once
}

// NewServer creates an Engine.IO v4 server. Nil options mean the defaults.
func NewServer(opts *Options) *Server {
	if opts == nil {
		opts = &Options{}
	}
	return &Server{
		opts:     opts.withDefaults(),
		sessions: make(map[string]*Session),
	}
}

func (srv *Server) removeSession(s *Session) {
	srv.mu.Lock()
	delete(srv.sessions, s.id)
	srv.mu.Unlock()
}

// Close ends every session (clients receive a close packet) and unblocks all
// parked polls. Requests already being written finish on their own.
func (srv *Server) Close() {
	srv.mu.Lock()
	sessions := make([]*Session, 0, len(srv.sessions))
	for _, s := range srv.sessions {
		sessions = append(sessions, s)
	}
	srv.mu.Unlock()
	for _, s := range sessions {
		s.mu.Lock()
		s.closeLocked(CloseServer)
		s.mu.Unlock()
	}
}

func (srv *Server) getSession(sid string) *Session {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.sessions[sid]
}

// warnNoMessageHandler guards the "no handler" warning so a noisy deploy
// logs one line, not one per packet.
func (srv *Server) warnNoMessageHandler() {
	srv.noMessageWarn.Do(func() {
		log.Println("engineio: message received but Server.OnMessage is not set; dropping")
	})
}

// serveHandshake starts a session and answers the initial GET with the Open
// packet. It never parks: the JS client waits for this response before
// anything else can happen.
func (srv *Server) serveHandshake(w http.ResponseWriter, r *http.Request) {
	s := newSession(srv)
	srv.mu.Lock()
	srv.sessions[s.id] = s
	srv.mu.Unlock()
	s.mu.Lock()
	s.armHeartbeatLocked()
	s.mu.Unlock()

	upgrades := []string{}
	if r.URL.Query().Get("transport") == "polling" {
		upgrades = []string{"websocket"}
	}
	body, err := srv.handshakeBody(s, upgrades)
	if err != nil {
		http.Error(w, "handshake encode failed", http.StatusInternalServerError)
		return
	}
	writePollPayload(w, []packet.Packet{{Type: packet.Open, Data: body}})

	if fn := srv.OnSession; fn != nil {
		fn(s)
	}
	go s.dispatch()
}

// ServeHTTP routes one Engine.IO HTTP request.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" && !srv.opts.allowsOrigin(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		if srv.opts.AllowCredentials {
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if headers := r.Header.Get("Access-Control-Request-Headers"); headers != "" {
			w.Header().Add("Vary", "Access-Control-Request-Headers")
			w.Header().Set("Access-Control-Allow-Headers", headers)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	q := r.URL.Query()
	if q.Get("EIO") != "4" {
		http.Error(w, "unsupported protocol version", http.StatusBadRequest)
		return
	}
	transport := q.Get("transport")
	sid := q.Get("sid")

	switch r.Method {
	case http.MethodGet:
		switch {
		case sid == "" && transport == "websocket":
			// A client may open a session straight on WebSocket, no
			// polling handshake first.
			srv.serveWebSocketRequest(w, r, "")
		case sid == "" && transport == "polling":
			srv.serveHandshake(w, r)
		case sid != "":
			s := srv.getSession(sid)
			if s == nil {
				http.Error(w, "unknown sid", http.StatusBadRequest)
				return
			}
			switch transport {
			case "polling":
				s.servePollingGet(w, r)
			case "websocket":
				srv.serveWebSocketRequest(w, r, sid)
			default:
				http.Error(w, "unknown transport", http.StatusBadRequest)
			}
		default:
			http.Error(w, "unknown transport", http.StatusBadRequest)
		}
	case http.MethodPost:
		if sid == "" || transport != "polling" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		s := srv.getSession(sid)
		if s == nil {
			http.Error(w, "unknown sid", http.StatusBadRequest)
			return
		}
		s.servePollingPost(w, r)
	default:
		http.Error(w, "invalid method", http.StatusBadRequest)
	}
}
