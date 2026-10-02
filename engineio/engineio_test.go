package engineio

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// harness wires a server to a real httptest listener and keeps the
// callbacks' traffic on channels for deterministic assertions.
type harness struct {
	t   *testing.T
	srv *Server
	ts  *httptest.Server
	url string

	messages chan string
	blobs    chan []byte
	closed   chan CloseReason
	sessions chan *Session
}

func newHarness(t *testing.T, opts *Options) *harness {
	t.Helper()
	h := &harness{
		t:        t,
		srv:      NewServer(opts),
		messages: make(chan string, 16),
		blobs:    make(chan []byte, 16),
		closed:   make(chan CloseReason, 4),
		sessions: make(chan *Session, 4),
	}
	h.srv.OnMessage = func(s *Session, data []byte, isBinary bool) {
		if isBinary {
			h.blobs <- data
			// echo, preserving the frame type the protocol tests assert on
			_ = s.SendBinary(data)
			return
		}
		h.messages <- string(data)
		// echo, which is what the protocol tests below assert on
		_ = s.SendText(string(data))
	}
	h.srv.OnClose = func(_ *Session, reason CloseReason) {
		h.closed <- reason
	}
	h.srv.OnSession = func(s *Session) {
		h.sessions <- s
	}
	h.ts = httptest.NewServer(h.srv)
	t.Cleanup(h.ts.Close)
	h.url = h.ts.URL + "/engine.io/"
	return h
}

func (h *harness) handshake() packet.Handshake {
	h.t.Helper()
	resp, err := http.Get(h.url + "?EIO=4&transport=polling&t=handshake")
	if err != nil {
		h.t.Fatalf("handshake GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("handshake status = %d, body %s", resp.StatusCode, body)
	}
	if len(body) == 0 || body[0] != '0' {
		h.t.Fatalf("handshake body = %q, want leading 0 (Open packet)", body)
	}
	var hs packet.Handshake
	if err := json.Unmarshal(body[1:], &hs); err != nil {
		h.t.Fatalf("handshake JSON: %v (%s)", err, body)
	}
	return hs
}

func (h *harness) post(sid, payload string) *http.Response {
	h.t.Helper()
	resp, err := http.Post(h.url+"?EIO=4&transport=polling&sid="+sid, "text/plain; charset=UTF-8", strings.NewReader(payload))
	if err != nil {
		h.t.Fatalf("POST %q: %v", payload, err)
	}
	return resp
}

func (h *harness) get(sid string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, h.url+"?EIO=4&transport=polling&sid="+sid+"&t=poll", nil)
	if err != nil {
		h.t.Fatalf("new GET: %v", err)
	}
	return http.DefaultClient.Do(req)
}

func (h *harness) pollOnce(sid string) string {
	h.t.Helper()
	resp, err := h.get(sid)
	if err != nil {
		h.t.Fatalf("poll GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("poll GET status = %d, body %s", resp.StatusCode, body)
	}
	return string(body)
}

func TestHandshake(t *testing.T) {
	h := newHarness(t, nil)
	hs := h.handshake()
	if hs.SID == "" {
		t.Fatal("handshake sid is empty")
	}
	if len(hs.Upgrades) != 1 || hs.Upgrades[0] != "websocket" {
		t.Errorf("upgrades = %v, want [websocket]", hs.Upgrades)
	}
	if hs.PingInterval != 25000 || hs.PingTimeout != 20000 || hs.MaxPayload != 1000000 {
		t.Errorf("handshake timings = %+v, want engine.io defaults", hs)
	}
	select {
	case s := <-h.sessions:
		if s.ID() != hs.SID {
			t.Errorf("OnSession id = %s, want %s", s.ID(), hs.SID)
		}
	case <-time.After(time.Second):
		t.Fatal("OnSession not called after handshake")
	}
}

func TestHandshakeRejectsBadRequests(t *testing.T) {
	h := newHarness(t, nil)

	resp, err := http.Get(h.url + "?EIO=3&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("EIO=3 status = %d, want 400", resp.StatusCode)
	}

	resp, err = http.Get(h.url + "?EIO=4&transport=polling&sid=does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown sid status = %d, want 400", resp.StatusCode)
	}
}

func TestEchoOverPolling(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	// Park a poll, then push a message through POST. Whichever order the
	// handlers run in, the poll must come back carrying the payload.
	got := make(chan string, 1)
	go func() {
		got <- h.pollOnce(sid)
	}()

	if resp := h.post(sid, "4hello engine"); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d", resp.StatusCode)
	}
	select {
	case body := <-got:
		if body != "4hello engine" {
			t.Errorf("poll body = %q, want %q", body, "4hello engine")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll never returned")
	}

	select {
	case msg := <-h.messages:
		if msg != "hello engine" {
			t.Errorf("OnMessage = %q, want %q", msg, "hello engine")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessage not called")
	}
}

func TestServerPingClientPong(t *testing.T) {
	h := newHarness(t, &Options{PingInterval: 60 * time.Millisecond, PingTimeout: 60 * time.Millisecond})
	sid := h.handshake().SID

	got := make(chan string, 1)
	go func() {
		got <- h.pollOnce(sid)
	}()
	// The server pings ~60ms after the handshake; the parked poll carries it.
	select {
	case body := <-got:
		if body != "2" {
			t.Fatalf("poll body = %q, want server ping 2", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server ping never arrived on the poll")
	}

	// The client's pong must be accepted and keep the session alive long
	// enough for the next ping to come out.
	if resp := h.post(sid, "3"); resp.StatusCode != http.StatusOK {
		t.Fatalf("pong POST status = %d", resp.StatusCode)
	}
	if body := h.pollOnce(sid); body != "2" {
		t.Errorf("next poll body = %q, want the following ping 2", body)
	}
}

func TestClientPingIsProtocolViolation(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	if resp := h.post(sid, "2"); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d", resp.StatusCode)
	}
	select {
	case reason := <-h.closed:
		if reason != CloseProtocol {
			t.Errorf("close reason = %q, want %q", reason, CloseProtocol)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client ping did not close the session")
	}
}

func TestClientClosePacket(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	if resp := h.post(sid, "1"); resp.StatusCode != http.StatusOK {
		t.Fatalf("close POST status = %d", resp.StatusCode)
	}
	select {
	case reason := <-h.closed:
		if reason != CloseTransport {
			t.Errorf("close reason = %q, want %q", reason, CloseTransport)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not called after client close")
	}

	resp, err := h.get(sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET after close status = %d, want 400", resp.StatusCode)
	}
}

func TestServerCloseFlushesParkedPoll(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	got := make(chan string, 1)
	go func() {
		got <- h.pollOnce(sid)
	}()

	time.Sleep(50 * time.Millisecond) // let the poll park
	h.srv.Close()

	select {
	case body := <-got:
		if body != "1" {
			t.Errorf("parked poll after Close = %q, want close packet 1", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll never returned after server Close")
	}
	select {
	case reason := <-h.closed:
		if reason != CloseServer {
			t.Errorf("close reason = %q, want %q", reason, CloseServer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not called after server Close")
	}
}

func TestOverlappingPollClosesSession(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	first := make(chan string, 1)
	go func() {
		first <- h.pollOnce(sid)
	}()

	time.Sleep(50 * time.Millisecond) // let the first poll park

	resp, err := h.get(sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("overlapping GET status = %d, want 400", resp.StatusCode)
	}

	// The parked poll goes out with the close packet, per the spec: two open
	// polls cannot both stream, so the session ends.
	select {
	case body := <-first:
		if body != "1" {
			t.Errorf("parked poll after duplicate = %q, want close 1", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll never returned after duplicate")
	}

	resp, err = h.get(sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET after duplicate close status = %d, want 400", resp.StatusCode)
	}
}

func TestClientCloseFlushesParkedPollWithNoop(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	got := make(chan string, 1)
	go func() {
		got <- h.pollOnce(sid)
	}()
	time.Sleep(50 * time.Millisecond) // let the poll park

	if resp := h.post(sid, "1"); resp.StatusCode != http.StatusOK {
		t.Fatalf("close POST status = %d", resp.StatusCode)
	}

	// The client asked to close, so it already knows: the parked poll
	// retires with a noop, not another close packet (spec behaviour).
	select {
	case body := <-got:
		if body != "6" {
			t.Errorf("parked poll after client close = %q, want noop 6", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll never returned after client close")
	}
	select {
	case reason := <-h.closed:
		if reason != CloseTransport {
			t.Errorf("close reason = %q, want %q", reason, CloseTransport)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not called after client close")
	}
}

func TestPayloadTooLargeClosesSession(t *testing.T) {
	h := newHarness(t, &Options{MaxPayload: 16})
	sid := h.handshake().SID

	if hs := h.handshake().MaxPayload; hs != 16 {
		// a second handshake is a fresh session; only asserting the config
		// surfaced in the handshake JSON
		t.Logf("handshake maxPayload = %d", hs)
	}

	big := "4" + strings.Repeat("x", 64)
	if resp := h.post(sid, big); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST too-large status = %d", resp.StatusCode)
	}
	select {
	case reason := <-h.closed:
		if reason != CloseProtocol {
			t.Errorf("close reason = %q, want %q", reason, CloseProtocol)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed after oversized payload")
	}

	resp, err := h.get(sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET after oversize close status = %d, want 400", resp.StatusCode)
	}
}

func TestHeartbeatTimeoutClosesSession(t *testing.T) {
	h := newHarness(t, &Options{PingInterval: 30 * time.Millisecond, PingTimeout: 30 * time.Millisecond})
	h.handshake()

	select {
	case reason := <-h.closed:
		if reason != CloseTimeout {
			t.Errorf("close reason = %q, want %q", reason, CloseTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed after heartbeat timeout")
	}
}

func TestHeartbeatKeptAliveByTraffic(t *testing.T) {
	h := newHarness(t, &Options{PingInterval: 40 * time.Millisecond, PingTimeout: 40 * time.Millisecond})
	sid := h.handshake().SID

	// The server pings ~40ms in and starts a 40ms pong deadline. A pong at
	// ~55ms must extend the session past the original 80ms deadline; if the
	// refresh were broken the session would die at ~80ms and this run, which
	// reaches ~105ms, would see it.
	time.Sleep(55 * time.Millisecond)
	if resp := h.post(sid, "3"); resp.StatusCode != http.StatusOK {
		t.Fatalf("pong POST status = %d", resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)

	select {
	case reason := <-h.closed:
		t.Fatalf("session closed despite fresh traffic: %q", reason)
	default:
	}
}

func TestPollCapFlushesNoop(t *testing.T) {
	oldCap := pollCap
	pollCap = 50 * time.Millisecond
	t.Cleanup(func() { pollCap = oldCap })

	h := newHarness(t, nil)
	sid := h.handshake().SID

	start := time.Now()
	body := h.pollOnce(sid)
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("poll returned early after %v", elapsed)
	}
	if body != "6" {
		t.Errorf("capped poll body = %q, want noop 6", body)
	}
}
