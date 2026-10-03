package socketio

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func ackSocket(t *testing.T) (*Server, *fakeSink, *Socket) {
	t.Helper()
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)
	return srv, f, srv.DefaultNamespace().FetchSockets()[0]
}

func closedAck(t *testing.T, ch <-chan []any) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected ack result")
		}
	case <-time.After(time.Second):
		t.Fatal("ack channel did not close")
	}
}

func TestAckClosesAfterResultAndIgnoresOtherNamespaces(t *testing.T) {
	_, f, s := ackSocket(t)
	ch := s.EmitWithAck("ask")
	f.clientSends(`3/never-connected,1["wrong"]`)
	select {
	case <-ch:
		t.Fatal("ack resolved across namespaces")
	default:
	}
	f.clientSends(`31["correct"]`)
	if args := <-ch; len(args) != 1 || args[0] != "correct" {
		t.Fatal(args)
	}
	closedAck(t, ch)
}

func TestAckCancellationRemovesWaiter(t *testing.T) {
	_, f, s := ackSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	ch := s.EmitWithAckContext(ctx, "ask")
	closedAck(t, ch)
	s.c.ackMu.Lock()
	count := len(s.c.ackWaits)
	s.c.ackMu.Unlock()
	if count != 0 {
		t.Fatalf("%d waiters remain after timeout", count)
	}
	f.clientSends(`31["late"]`)
	closedAck(t, ch)
	before := len(f.drain())
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	closedAck(t, s.EmitWithAckContext(ctx, "canceled"))
	if len(f.drain()) != before {
		t.Fatal("already canceled emit was sent")
	}
}

func TestNamespaceDisconnectClosesOnlyItsOwnWaiters(t *testing.T) {
	for _, serverSide := range []bool{false, true} {
		t.Run(strconv.FormatBool(serverSide), func(t *testing.T) {
			srv, f, s := ackSocket(t)
			admin := srv.Of("/admin")
			f.clientSends("0/admin,")
			mainWait := s.EmitWithAck("main")
			adminWait := admin.FetchSockets()[0].EmitWithAck("admin")
			if serverSide {
				s.Disconnect()
			} else {
				f.clientSends("1")
			}
			closedAck(t, mainWait)
			select {
			case <-adminWait:
				t.Fatal("peer namespace waiter canceled")
			default:
			}
			f.clientSends(`3/admin,2["ok"]`)
			if args := <-adminWait; args[0] != "ok" {
				t.Fatal(args)
			}
			closedAck(t, adminWait)
			closedAck(t, s.EmitWithAck("disconnected"))
		})
	}
}

type failingAckSink struct {
	*fakeSink
	entered, release chan struct{}
}

func (f *failingAckSink) SendText(string) error {
	close(f.entered)
	<-f.release
	return errors.New("failed write")
}

func TestFailedAckSendConcurrentWithDisconnectDoesNotPanic(t *testing.T) {
	srv, f, s := ackSocket(t)
	failing := &failingAckSink{fakeSink: f, entered: make(chan struct{}), release: make(chan struct{})}
	s.c.sess = failing
	srv.mu.Lock()
	delete(srv.clients, f)
	srv.clients[failing] = s.c
	srv.mu.Unlock()
	finished := make(chan struct{})
	go func() { defer close(finished); closedAck(t, s.EmitWithAck("ask")) }()
	<-failing.entered
	s.c.kill(reasonTransportClose)
	close(failing.release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("send did not complete")
	}
}

func TestAckAndCancellationRaceCompleteExactlyOnce(t *testing.T) {
	_, f, s := ackSocket(t)
	for i := 1; i <= 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch := s.EmitWithAckContext(ctx, "ask")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); f.clientSends("3" + strconv.Itoa(i) + `["ok"]`) }()
		wg.Wait()
		for range ch {
		} // either one answer or an empty canceled channel
	}
	s.c.ackMu.Lock()
	count := len(s.c.ackWaits)
	s.c.ackMu.Unlock()
	if count != 0 {
		t.Fatal(count)
	}
}

func TestParseErrorClosesUnderlyingTransport(t *testing.T) {
	srv, f, _ := ackSocket(t)
	f.clientSends("invalid")
	select {
	case <-f.Done():
	default:
		t.Fatal("parse error left transport open")
	}
	if len(srv.clients) != 0 {
		t.Fatal("client remains registered")
	}
}

func TestExcessiveAndInvalidBinaryHeadersAreRejectedBeforeBuffering(t *testing.T) {
	for _, header := range []string{`5999999999-["blob"]`, `51-["blob"]`, `51-["blob",{"_placeholder":true,"num":3}]`} {
		t.Run(header, func(t *testing.T) {
			_, f, s := ackSocket(t)
			f.clientSends(header)
			select {
			case <-f.Done():
			default:
				t.Fatal("invalid header accepted")
			}
			if s.c.needBin != 0 || len(s.c.bins) != 0 {
				t.Fatal("attachment buffer retained")
			}
		})
	}
}
