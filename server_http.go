package socketio

import (
	"net/http"

	"github.com/somprasongd/go-socketio-v4/engineio"
)

// New creates a Socket.IO server wired to its own engine.io endpoint.
// The options configure the engine.io layer (timings, max payload); nil
// means the engine.io defaults. Mount the returned server at any path —
// conventionally /socket.io/ — and register handlers on its namespaces
// before serving.
func New(opts *engineio.Options) *Server {
	core := newServer()
	core.eio = engineio.NewServer(opts)
	core.eio.OnSession = func(s *engineio.Session) { core.attach(s) }
	core.eio.OnMessage = func(s *engineio.Session, data []byte, isBinary bool) {
		core.dispatch(s, data, isBinary)
	}
	core.eio.OnClose = func(s *engineio.Session, reason engineio.CloseReason) {
		core.detach(s, mapCloseReason(reason))
	}
	return core
}

// EngineIO exposes the underlying engine.io server (to tweak its options or
// mount it separately).
func (srv *Server) EngineIO() *engineio.Server { return srv.eio }

// ServeHTTP serves both layers at the mount path: the engine.io handshake,
// polling, WebSocket upgrades and the Socket.IO packets inside them.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.eio.ServeHTTP(w, r)
}
