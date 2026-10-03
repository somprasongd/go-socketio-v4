package socketio

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/somprasongd/go-socketio-v4/parser"
)

func recoveryIdentity(t *testing.T, f *fakeSink) (string, string) {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(f.last(), "0")), &p); err != nil {
		t.Fatal(err)
	}
	return p["sid"].(string), p["pid"].(string)
}

func TestRecoveryVolatileAndAckDoNotAdvanceOffset(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	_, pid := recoveryIdentity(t, f)
	s := ns.FetchSockets()[0]
	_ = s.Emit("seed")
	_ = s.Volatile().Emit("volatile")
	_ = s.EmitWithAck("acked")
	_ = s.Emit("next")
	s.rec.mu.Lock()
	count, off := len(s.rec.buffer), s.rec.offset
	s.rec.mu.Unlock()
	if count != 2 || off != 2 {
		t.Fatalf("buffer=%d offset=%d", count, off)
	}
	srv.detach(f, reasonTransportClose)
	restored := newFakeSink(srv)
	restored.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	wire := strings.Join(restored.drain(), "\n")
	if strings.Contains(wire, "volatile") || strings.Contains(wire, "acked") || !strings.Contains(wire, "next") {
		t.Fatal(wire)
	}
}

func TestRecoveryInvalidOrEvictedOffsetsFallBack(t *testing.T) {
	for _, offset := range []string{"", "bad", "0", "-1", "999", "1"} {
		t.Run(offset, func(t *testing.T) {
			srv := newServer()
			ns := srv.DefaultNamespace()
			ns.EnableRecovery(&RecoveryOptions{MaxBufferedEvents: 2})
			f := newFakeSink(srv)
			connectWithAuth(t, f, "")
			sid, pid := recoveryIdentity(t, f)
			s := ns.FetchSockets()[0]
			_ = s.Emit("one")
			_ = s.Emit("two")
			_ = s.Emit("three")
			srv.detach(f, reasonTransportClose)
			next := newFakeSink(srv)
			next.clientSends(`0{"pid":"` + pid + `","offset":"` + offset + `"}`)
			newSID, _ := recoveryIdentity(t, next)
			if sid == newSID {
				t.Fatal("invalid history reported recovered")
			}
		})
	}
}

func TestRecoveryMiddlewareRejectsOnceAndAllowsEmit(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	_, pid := recoveryIdentity(t, f)
	s := ns.FetchSockets()[0]
	_ = s.Emit("seed")
	srv.detach(f, reasonTransportClose)
	calls := 0
	ns.Use(func(s *Socket) error { calls++; ns.Emit("during-middleware"); return errors.New("blocked") })
	next := newFakeSink(srv)
	next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	if calls != 1 || len(ns.FetchSockets()) != 0 || !strings.HasPrefix(next.last(), "4") {
		t.Fatalf("calls=%d sockets=%d wire=%v", calls, len(ns.FetchSockets()), next.drain())
	}
}

func TestRecoveryReplayPrecedesOnConnectAndConcurrentEmit(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	_, pid := recoveryIdentity(t, f)
	s := ns.FetchSockets()[0]
	_ = s.Emit("seed")
	srv.detach(f, reasonTransportClose)
	ns.Emit("missed")
	ns.Use(func(s *Socket) error {
		if s.Recovered() {
			ns.Emit("middleware-event")
		}
		return nil
	})
	ns.OnConnect(func(s *Socket) {
		if s.Recovered() {
			ns.Emit("live")
			_ = s.Emit("from-handler")
		}
	})
	next := newFakeSink(srv)
	next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	wire := strings.Join(next.drain(), "\n")
	if strings.Index(wire, "missed") > strings.Index(wire, "from-handler") || strings.Index(wire, "middleware-event") > strings.Index(wire, "live") {
		t.Fatal(wire)
	}
}

func TestRecoveryCustomNamespaceBinaryHeld(t *testing.T) {
	srv := newServer()
	ns := srv.Of("/admin")
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	f.clientSends("0/admin,")
	p, err := parser.Decode(f.last(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pid := p.Data.(map[string]any)["pid"].(string)
	_ = ns.FetchSockets()[0].Emit("seed")
	srv.detach(f, reasonTransportClose)
	ns.Emit("binary", []byte{1, 2, 3})
	next := newFakeSink(srv)
	next.clientSends(`0/admin,{"pid":"` + pid + `","offset":"1"}`)
	wire := next.drain()
	if len(wire) != 3 || !strings.Contains(wire[1], "/admin,") || wire[2] != "bin:\x01\x02\x03" {
		t.Fatalf("wire=%q", wire)
	}
}

func TestRecoveryConcurrentClaimsRestoreOnlyOnce(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	sid, pid := recoveryIdentity(t, f)
	_ = ns.FetchSockets()[0].Emit("seed")
	srv.detach(f, reasonTransportClose)
	var wg sync.WaitGroup
	sinks := []*fakeSink{newFakeSink(srv), newFakeSink(srv)}
	for _, next := range sinks {
		wg.Go(func() { next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`) })
	}
	wg.Wait()
	recovered := 0
	for _, next := range sinks {
		got, _ := recoveryIdentity(t, next)
		if got == sid {
			recovered++
		}
	}
	if recovered != 1 {
		t.Fatalf("restored %d times", recovered)
	}
}

func TestRejectedMiddlewareCannotLeakRoomMembership(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.Use(func(s *Socket) error { s.Join("secret"); return errors.New("rejected") })
	f := newFakeSink(srv)
	f.clientSends("0")
	ns.To("secret").Emit("leaked")
	if strings.Contains(strings.Join(f.drain(), "\n"), "leaked") || len(ns.adapterOf().Members([]string{"secret"})) != 0 {
		t.Fatal("rejected socket leaked into room")
	}
}

func TestRecoveryMiddlewareRoomChangesApplyOnlyAfterAcceptance(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	_, pid := recoveryIdentity(t, f)
	s := ns.FetchSockets()[0]
	s.Join("before")
	_ = s.Emit("seed")
	srv.detach(f, reasonTransportClose)
	ns.Use(func(s *Socket) error {
		if s.Recovered() {
			s.Leave("before")
			s.Join("after")
		}
		return nil
	})
	next := newFakeSink(srv)
	next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	ns.To("before").Emit("wrong")
	ns.To("after").Emit("right")
	wire := strings.Join(next.drain(), "\n")
	if strings.Contains(wire, "wrong") || !strings.Contains(wire, "right") {
		t.Fatal(wire)
	}
}

func TestRecoveryHistoryEvictedDuringMiddlewareFallsBack(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(&RecoveryOptions{MaxBufferedEvents: 2})
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	sid, pid := recoveryIdentity(t, f)
	_ = ns.FetchSockets()[0].Emit("seed")
	srv.detach(f, reasonTransportClose)
	ns.Use(func(s *Socket) error {
		if s.Recovered() {
			ns.Emit("evict-one")
			ns.Emit("evict-two")
		}
		return nil
	})
	next := newFakeSink(srv)
	next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	fresh, _ := recoveryIdentity(t, next)
	if fresh == sid {
		t.Fatal("incomplete history reported recovered")
	}
}

func TestRecoveryBinaryBufferOwnsAttachments(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	_, pid := recoveryIdentity(t, f)
	_ = ns.FetchSockets()[0].Emit("seed")
	srv.detach(f, reasonTransportClose)
	data := []byte("keep")
	ns.Emit("held-binary", data)
	copy(data, []byte("lost"))
	next := newFakeSink(srv)
	next.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	if next.last() != "bin:keep" {
		t.Fatalf("borrowed mutable replay buffer: %q", next.drain())
	}
}
