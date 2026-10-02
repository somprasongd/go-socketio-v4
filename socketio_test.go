package socketio

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSink is an in-memory engine.io session: it records what the server
// sends and lets tests push client packets in.
type fakeSink struct {
	id   string
	mu   sync.Mutex
	sent []string // wire pieces in order: text packets and hex-tagged binaries
	done chan struct{}
	srv  *Server
}

func newFakeSink(srv *Server) *fakeSink {
	s := &fakeSink{id: "fake-1", done: make(chan struct{}), srv: srv}
	srv.attach(s)
	return s
}

func (f *fakeSink) ID() string { return f.id }

func (f *fakeSink) SendText(text string) error {
	f.mu.Lock()
	f.sent = append(f.sent, text)
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) SendBinary(b []byte) error {
	f.mu.Lock()
	f.sent = append(f.sent, "bin:"+string(b))
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) Done() <-chan struct{} { return f.done }

func (f *fakeSink) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.done:
	default:
		close(f.done)
	}
}

// client sends a raw text packet as if it came from the client.
func (f *fakeSink) clientSends(text string) {
	f.srv.dispatch(f, []byte(text), false)
}

func (f *fakeSink) clientSendsBinary(b []byte) {
	f.srv.dispatch(f, b, true)
}

// drain returns everything the server has written so far.
func (f *fakeSink) drain() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.sent))
	copy(out, f.sent)
	return out
}

// last returns the most recent server write.
func (f *fakeSink) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) == 0 {
		return ""
	}
	return f.sent[len(f.sent)-1]
}

// connectClient performs the default-namespace handshake and returns the
// server-assigned socket id.
func connectClient(t *testing.T, f *fakeSink) (sid string) {
	t.Helper()
	f.clientSends("0")
	last := f.last()
	if !strings.HasPrefix(last, "0") {
		t.Fatalf("connect reply = %q, want a CONNECT packet", last)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(last[1:]), &payload); err != nil {
		t.Fatalf("connect reply JSON: %v", err)
	}
	sid, _ = payload["sid"].(string)
	if sid == "" {
		t.Fatalf("connect reply missing sid: %q", last)
	}
	return sid
}

func TestConnectHandshake(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	var connected *Socket
	connectedCh := make(chan struct{})
	srv.DefaultNamespace().OnConnect(func(s *Socket) {
		connected = s
		close(connectedCh)
	})

	sid := connectClient(t, f)

	select {
	case <-connectedCh:
		if connected.ID() != sid {
			t.Errorf("handler socket id = %s, want %s", connected.ID(), sid)
		}
	case <-time.After(time.Second):
		t.Fatal("OnConnect never fired")
	}
}

func TestConnectUnknownNamespaceRefused(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)

	f.clientSends("0/missing,")
	last := f.last()
	if !strings.HasPrefix(last, "4/missing,") {
		t.Fatalf("reply = %q, want CONNECT_ERROR for /missing", last)
	}
	if !strings.Contains(last, "Invalid namespace") {
		t.Errorf("reply = %q, want an explanatory message", last)
	}
}

func TestEventWithAckBack(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	handlerDone := make(chan []any, 1)
	srv.DefaultNamespace().OnEvent("greet", func(s *Socket, args []any, ack func(response ...any)) {
		handlerDone <- args
		if ack != nil {
			ack("hi", float64(42))
		}
	})

	connectClient(t, f)
	f.clientSends(`21["greet","someone"]`)

	select {
	case args := <-handlerDone:
		if len(args) != 1 || args[0] != "someone" {
			t.Errorf("handler args = %#v", args)
		}
	case <-time.After(time.Second):
		t.Fatal("event handler never ran")
	}
	last := f.last()
	if last != `31["hi",42]` {
		t.Errorf("ack = %q, want %q", last, `31["hi",42]`)
	}
}

func TestEventWithoutAckIdGetsNoReply(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	srv.DefaultNamespace().OnEvent("ping", func(s *Socket, args []any, ack func(response ...any)) {
		if ack != nil {
			t.Error("ack should be nil for an event without an ack id")
		}
	})
	connectClient(t, f)
	before := len(f.drain())

	f.clientSends(`2["ping"]`)
	if got := len(f.drain()); got != before {
		t.Errorf("server wrote %d packets for an ack-less event, want 0", got-before)
	}
}

func TestServerEmitWithAck(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)

	var sock *Socket
	srv.DefaultNamespace().OnConnect(func(s *Socket) { sock = s })

	// re-run connect to capture the socket (OnConnect fired during
	// connectClient; fetch it from the namespace instead)
	socks := srv.DefaultNamespace().FetchSockets()
	if len(socks) != 1 {
		t.Fatalf("sockets = %d", len(socks))
	}
	sock = socks[0]

	result := sock.EmitWithAck("who-are-you")
	// The wire must show the ack id 1 on the event.
	if last := f.last(); last != `21["who-are-you"]` {
		t.Fatalf("emit = %q, want ack id 1", last)
	}

	// The client answers.
	f.clientSends(`31["me","too"]`)
	select {
	case args := <-result:
		if len(args) != 2 || args[0] != "me" {
			t.Errorf("ack args = %#v", args)
		}
	case <-time.After(time.Second):
		t.Fatal("ack never arrived")
	}
}

func TestRoomsAndBroadcast(t *testing.T) {
	srv := newServer()

	f1 := newFakeSink(srv)
	f1.clientSends("0")
	f2 := newFakeSink(srv)
	f2.clientSends("0")

	socks := srv.DefaultNamespace().FetchSockets()
	if len(socks) != 2 {
		t.Fatalf("sockets = %d, want 2", len(socks))
	}
	socks[0].Join("card-readers")

	srv.DefaultNamespace().To("card-readers").Emit("hello-room")
	if last := f1.last(); last != `2["hello-room"]` {
		t.Errorf("room member got %q", last)
	}
	for _, m := range f2.drain() {
		if strings.Contains(m, "hello-room") {
			t.Errorf("non-member received room broadcast: %q", m)
		}
	}

	// Broadcast excludes the sender.
	socks[1].Broadcast().Emit("from-2")
	f2texts := f2.drain()
	for _, m := range f2texts {
		if strings.Contains(m, "from-2") {
			t.Errorf("sender received its own broadcast: %q", m)
		}
	}
	found := false
	for _, m := range f1.drain() {
		if strings.Contains(m, "from-2") {
			found = true
		}
	}
	if !found {
		t.Error("peer never received the broadcast")
	}

	// socket.To(room) = room minus me.
	socks[0].Join("pair")
	socks[1].Join("pair")
	socks[0].To("pair").Emit("only-2")
	if !strings.Contains(strings.Join(f2.drain(), "\n"), "only-2") {
		t.Error("room peer missed the targeted emit")
	}
	for _, m := range f1.drain() {
		if strings.Contains(m, "only-2") {
			t.Error("sender got its own room emit")
		}
	}
}

func TestNamespaceDisconnectByClient(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	sid := connectClient(t, f)

	disconnected := make(chan string, 1)
	srv.DefaultNamespace().OnDisconnect(func(s *Socket, reason string) {
		disconnected <- reason
	})

	f.clientSends("1")
	select {
	case reason := <-disconnected:
		if reason != reasonClientDisconnect {
			t.Errorf("reason = %q, want %q", reason, reasonClientDisconnect)
		}
	case <-time.After(time.Second):
		t.Fatal("OnDisconnect never fired")
	}
	if n := len(srv.DefaultNamespace().FetchSockets()); n != 0 {
		t.Errorf("sockets left = %d, want 0 (sid was %s)", n, sid)
	}
}

func TestTransportCloseNotifiesNamespace(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)

	disconnected := make(chan string, 1)
	srv.DefaultNamespace().OnDisconnect(func(s *Socket, reason string) {
		disconnected <- reason
	})

	srv.detach(f, reasonTransportClose)
	select {
	case reason := <-disconnected:
		if reason != reasonTransportClose {
			t.Errorf("reason = %q, want %q", reason, reasonTransportClose)
		}
	case <-time.After(time.Second):
		t.Fatal("OnDisconnect never fired")
	}
}

func TestServerSideDisconnect(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)

	disconnected := make(chan string, 1)
	srv.DefaultNamespace().OnDisconnect(func(s *Socket, reason string) {
		disconnected <- reason
	})

	socks := srv.DefaultNamespace().FetchSockets()
	socks[0].Disconnect()

	if last := f.last(); last != "1" {
		t.Errorf("wire = %q, want a disconnect packet 1", last)
	}
	select {
	case reason := <-disconnected:
		if reason != reasonServerDisconnect {
			t.Errorf("reason = %q, want %q", reason, reasonServerDisconnect)
		}
	case <-time.After(time.Second):
		t.Fatal("OnDisconnect never fired")
	}
}

func TestBinaryEventAssemblesAttachments(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	got := make(chan []any, 1)
	srv.DefaultNamespace().OnEvent("photo", func(s *Socket, args []any, ack func(response ...any)) {
		got <- args
	})

	connectClient(t, f)
	// text part declares one attachment…
	f.clientSends(`51-["photo",{"_placeholder":true,"num":0}]`)
	// …then the binary engine.io packet follows.
	f.clientSendsBinary([]byte{0xde, 0xad, 0xbe, 0xef})

	select {
	case args := <-got:
		blob, ok := args[0].([]byte)
		if !ok || !bytes.Equal(blob, []byte{0xde, 0xad, 0xbe, 0xef}) {
			t.Fatalf("binary arg = %#v", args[0])
		}
	case <-time.After(time.Second):
		t.Fatal("binary event never delivered")
	}
}

func TestBinaryEmitSendsAttachments(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)
	socks := srv.DefaultNamespace().FetchSockets()

	_ = socks[0].Emit("snapshot", []byte{1, 2, 3})
	sent := f.drain()
	// [0] is the connect handshake reply; the binary event is text+attachment.
	if len(sent) != 3 {
		t.Fatalf("wire pieces = %d, want handshake+text+attachment", len(sent))
	}
	if sent[1] != `51-["snapshot",{"_placeholder":true,"num":0}]` {
		t.Errorf("text = %q", sent[1])
	}
	if sent[2] != "bin:"+string([]byte{1, 2, 3}) {
		t.Errorf("attachment = %q", sent[2])
	}
}

func TestParseErrorKillsClient(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	disconnected := make(chan string, 1)
	srv.DefaultNamespace().OnDisconnect(func(s *Socket, reason string) {
		disconnected <- reason
	})

	connectClient(t, f)
	f.clientSends(`9["garbage"]`)

	select {
	case reason := <-disconnected:
		if reason != reasonParseError {
			t.Errorf("reason = %q, want %q", reason, reasonParseError)
		}
	case <-time.After(time.Second):
		t.Fatal("client was not killed on a parse error")
	}
}

func TestOfCreatesNamespaces(t *testing.T) {
	srv := newServer()
	admin := srv.Of("/admin")
	if admin.Name() != "/admin" {
		t.Fatalf("name = %q", admin.Name())
	}
	if srv.Of("admin") != admin {
		t.Error("Of should normalise a missing leading slash")
	}
	if srv.Of("/") != srv.DefaultNamespace() {
		t.Error("Of(/) must return the default namespace")
	}
}
