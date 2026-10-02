// Package socketio implements the Socket.IO v4 server on top of the
// engineio package: namespaces, events with acknowledgements, rooms and
// broadcast.
//
// Spec: https://socket.io/docs/v4/socket-io-protocol/
package socketio

import (
	"sync"

	"github.com/somprasongd/go-socketio-v4/engineio"
)

// sendSink is the slice of an engine.io session the socket.io layer sends
// through. Kept as an interface so tests can drive the server with a fake
// connection.
type sendSink interface {
	ID() string
	SendText(string) error
	SendBinary([]byte) error
	Done() <-chan struct{}
	Close()
}

// Server is a Socket.IO v4 endpoint: a set of namespaces fed by engine.io
// sessions. Construct with New, which wires the engine.io transport, then
// register handlers on its namespaces before serving.
type Server struct {
	mu      sync.Mutex
	nsps    map[string]*Namespace
	clients map[sendSink]*client

	eio *engineio.Server
}

// newServer creates the server core with its default ("/") namespace.
func newServer() *Server {
	srv := &Server{
		nsps:    make(map[string]*Namespace),
		clients: make(map[sendSink]*client),
	}
	srv.nsps["/"] = newNamespace(srv, "/")
	return srv
}

// Of returns the namespace with the given name ("/admin" style), creating
// it on first use.
func (srv *Server) Of(name string) *Namespace {
	if name == "" || name == "/" {
		return srv.DefaultNamespace()
	}
	if name[0] != '/' {
		name = "/" + name
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	ns := srv.nsps[name]
	if ns == nil {
		ns = newNamespace(srv, name)
		srv.nsps[name] = ns
	}
	return ns
}

// DefaultNamespace returns the "/" namespace.
func (srv *Server) DefaultNamespace() *Namespace {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.nsps["/"]
}

// attach registers a transport session as a Socket.IO client. Called by the
// engine.io OnSession hook.
func (srv *Server) attach(sess sendSink) {
	c := newClient(srv, sess)
	srv.mu.Lock()
	srv.clients[sess] = c
	srv.mu.Unlock()
}

// detach ends every namespace connection the client holds. Called from the
// engine.io OnClose hook; reason is already in socket.io vocabulary.
func (srv *Server) detach(sess sendSink, reason string) {
	srv.mu.Lock()
	c := srv.clients[sess]
	srv.mu.Unlock()
	if c != nil {
		c.kill(reason)
	}
}

// dispatch feeds one engine.io Message packet to the client.
func (srv *Server) dispatch(sess sendSink, data []byte, isBinary bool) {
	srv.mu.Lock()
	c := srv.clients[sess]
	srv.mu.Unlock()
	if c != nil {
		c.onMessage(data, isBinary)
	}
}

// removeClient drops the client registration once its last lock holder is
// done with it.
func (srv *Server) removeClient(sess sendSink) {
	srv.mu.Lock()
	delete(srv.clients, sess)
	srv.mu.Unlock()
}

// namespaceLocked looks a namespace up by name; nil when unknown, which the
// caller answers with a CONNECT_ERROR.
func (srv *Server) namespaceLocked(name string) *Namespace {
	return srv.nsps[name]
}
