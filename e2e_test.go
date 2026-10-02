package socketio

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func httpGet(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

func httpPost(url, body string) error {
	resp, err := http.Post(url, "text/plain; charset=UTF-8", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return nil
}

// e2e drives the whole stack — engine.io WebSocket transport, the parser and
// the namespace core — with a raw client speaking wire packets, the way a
// real browser would.
type e2eClient struct {
	t   *testing.T
	c   *websocket.Conn
	sid string // engine.io session id
}

func startE2E(t *testing.T) (*Server, *e2eClient) {
	t.Helper()
	srv := New(nil)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	e := &e2eClient{t: t, c: c}

	// engine.io handshake frame: 0{...}
	frame := e.readFrame()
	if !strings.HasPrefix(frame, "0") {
		t.Fatalf("first frame = %q, want engine.io OPEN", frame)
	}
	sid := frame[strings.Index(frame, `"sid":"`)+len(`"sid":"`):]
	e.sid = sid[:strings.IndexByte(sid, '"')]
	return srv, e
}

func (e *e2eClient) readFrame() string {
	e.t.Helper()
	_ = e.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, data, err := e.c.ReadMessage()
	if err != nil {
		e.t.Fatalf("read: %v", err)
	}
	if mt == websocket.BinaryMessage {
		return "bin:" + string(data)
	}
	return string(data)
}

func (e *e2eClient) send(frame string) {
	e.t.Helper()
	if err := e.c.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
		e.t.Fatalf("write %q: %v", frame, err)
	}
}

func (e *e2eClient) sendBinary(b []byte) {
	e.t.Helper()
	if err := e.c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		e.t.Fatalf("binary write: %v", err)
	}
}

func TestE2EFullFlow(t *testing.T) {
	srv, e := startE2E(t)
	ns := srv.DefaultNamespace()

	connected := make(chan *Socket, 1)
	ns.OnConnect(func(s *Socket) { connected <- s })
	ns.OnEvent("greet", func(s *Socket, args []any, ack func(response ...any)) {
		if ack != nil {
			ack("hello", args[0])
		}
	})
	binarySeen := make(chan []byte, 1)
	ns.OnEvent("blob", func(s *Socket, args []any, ack func(response ...any)) {
		if b, ok := args[0].([]byte); ok {
			binarySeen <- b
		}
	})

	// namespace connect
	e.send("40") // engine.io MESSAGE carrying socket.io CONNECT
	frame := e.readFrame()
	if !strings.HasPrefix(frame, `40{"sid":"`) {
		t.Fatalf("namespace connect reply = %q", frame)
	}

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnect never fired")
	}

	// server → client push
	socks := ns.FetchSockets()
	socks[0].Emit("news", "payload")
	if frame := e.readFrame(); frame != `42["news","payload"]` {
		t.Errorf("push frame = %q", frame)
	}

	// client event with ack
	e.send(`421["greet","world"]`)
	if frame := e.readFrame(); frame != `431["hello","world"]` {
		t.Errorf("ack frame = %q", frame)
	}

	// client binary event: text then attachment frame
	e.send(`451-["blob",{"_placeholder":true,"num":0}]`)
	e.sendBinary([]byte{0xaa, 0xbb})
	select {
	case b := <-binarySeen:
		if !bytes.Equal(b, []byte{0xaa, 0xbb}) {
			t.Errorf("binary arg = %x", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("binary event never delivered")
	}

	// server binary push arrives as text + binary frames
	socks[0].Emit("snapshot", []byte{1, 2, 3})
	if frame := e.readFrame(); frame != `451-["snapshot",{"_placeholder":true,"num":0}]` {
		t.Errorf("binary push text = %q", frame)
	}
	if frame := e.readFrame(); frame != "bin:"+string([]byte{1, 2, 3}) {
		t.Errorf("binary push attachment = %q", frame)
	}

	// namespace broadcast
	ns.Emit("to-all")
	if frame := e.readFrame(); frame != `42["to-all"]` {
		t.Errorf("broadcast frame = %q", frame)
	}

	// client-side disconnect notifies OnDisconnect with the right reason
	disconnected := make(chan string, 1)
	ns.OnDisconnect(func(s *Socket, reason string) { disconnected <- reason })
	e.send("41") // engine.io MESSAGE carrying socket.io DISCONNECT
	select {
	case reason := <-disconnected:
		if reason != reasonClientDisconnect {
			t.Errorf("disconnect reason = %q", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnDisconnect never fired")
	}
}

func TestE2EPollingTransport(t *testing.T) {
	srv := New(nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	base := ts.URL + "/socket.io/?EIO=4&transport=polling"

	got := make(chan string, 1)
	srv.DefaultNamespace().OnEvent("hi", func(s *Socket, args []any, ack func(response ...any)) {
		got <- "event reached the core"
	})

	// handshake over polling
	resp, err := httpGet(base)
	if err != nil {
		t.Fatal(err)
	}
	sid := resp[strings.Index(resp, `"sid":"`)+len(`"sid":"`):]
	sid = sid[:strings.IndexByte(sid, '"')]

	// connect over polling POST
	if err := httpPost(base+"&sid="+sid, "40"); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("namespace connect reply never polled back")
		default:
		}
		body, _ := httpGet(base + "&sid=" + sid)
		if strings.HasPrefix(body, `40{"sid"`) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// send an event over polling
	if err := httpPost(base+"&sid="+sid, `42["hi"]`); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if msg != "event reached the core" {
			t.Errorf("msg = %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event never delivered over polling")
	}
}
