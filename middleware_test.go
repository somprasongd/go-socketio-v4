package socketio

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMiddlewareAcceptsAndSurfacesAuth(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	ns := srv.DefaultNamespace()

	var seenAuth map[string]any
	mwDone := make(chan struct{})
	ns.Use(func(s *Socket) error {
		seenAuth = s.Handshake()
		close(mwDone)
		return nil
	})
	connected := make(chan *Socket, 1)
	ns.OnConnect(func(s *Socket) { connected <- s })

	f.clientSends(`0{"token":"sekrit"}`)
	select {
	case <-mwDone:
	case <-time.After(time.Second):
		t.Fatal("middleware never ran")
	}
	if seenAuth == nil || seenAuth["token"] != "sekrit" {
		t.Errorf("handshake auth = %#v", seenAuth)
	}
	select {
	case s := <-connected:
		if s.Handshake()["token"] != "sekrit" {
			t.Errorf("post-connect Handshake = %#v", s.Handshake())
		}
	case <-time.After(time.Second):
		t.Fatal("OnConnect never fired")
	}
}

func TestMiddlewareRejects(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	ns := srv.DefaultNamespace()

	ns.Use(func(s *Socket) error {
		if s.Handshake()["token"] != "sekrit" {
			return errors.New("invalid credentials")
		}
		return nil
	})
	connectFired := make(chan struct{}, 1)
	ns.OnConnect(func(s *Socket) { connectFired <- struct{}{} })

	// wrong token → CONNECT_ERROR, no connection
	f.clientSends(`0{"token":"wrong"}`)
	last := f.last()
	if !strings.HasPrefix(last, "4") || !strings.Contains(last, "invalid credentials") {
		t.Errorf("reply = %q, want CONNECT_ERROR carrying the middleware message", last)
	}
	select {
	case <-connectFired:
		t.Fatal("OnConnect fired for a refused socket")
	case <-time.After(150 * time.Millisecond):
	}
	if n := len(ns.FetchSockets()); n != 0 {
		t.Errorf("sockets registered = %d, want 0", n)
	}

	// right token → connects
	f.clientSends(`0{"token":"sekrit"}`)
	select {
	case <-connectFired:
	case <-time.After(time.Second):
		t.Fatal("correct token was refused")
	}
}

func TestMiddlewareRunsInOrder(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	ns := srv.DefaultNamespace()

	var order []string
	ns.Use(func(s *Socket) error { order = append(order, "first"); return nil })
	ns.Use(func(s *Socket) error { order = append(order, "second"); return nil })

	f.clientSends("0")
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Errorf("middleware order = %v", order)
	}
}

func TestFirstMiddlewareErrorStopsChain(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	ns := srv.DefaultNamespace()

	secondRan := false
	ns.Use(func(s *Socket) error { return errors.New("stop here") })
	ns.Use(func(s *Socket) error { secondRan = true; return nil })

	f.clientSends("0")
	if secondRan {
		t.Error("middleware after the failing one still ran")
	}
	if !strings.Contains(f.last(), "stop here") {
		t.Errorf("reply = %q", f.last())
	}
}
