package engineio

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// dialWS opens a client WebSocket connection to the harness's engine.io path.
func (h *harness) dialWS(query string) *websocket.Conn {
	h.t.Helper()
	wsURL := "ws" + strings.TrimPrefix(h.ts.URL, "http") + "/engine.io/?" + query
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		h.t.Fatalf("ws dial: %v", err)
	}
	return c
}

// readFrame reads one text frame with a deadline and returns its payload.
func readFrame(t *testing.T, c *websocket.Conn) (int, string, []byte) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	_ = c.SetReadDeadline(time.Time{})
	return mt, string(data), data
}

func writeText(t *testing.T, c *websocket.Conn, s string) {
	t.Helper()
	if err := c.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
		t.Fatalf("ws write %q: %v", s, err)
	}
}

func TestWebSocketOnlySession(t *testing.T) {
	h := newHarness(t, nil)
	c := h.dialWS("EIO=4&transport=websocket")

	mt, text, _ := readFrame(t, c)
	if mt != websocket.TextMessage || text[0] != '0' {
		t.Fatalf("first frame = (%v, %q), want text Open packet", mt, text)
	}
	var hs packet.Handshake
	if err := json.Unmarshal([]byte(text[1:]), &hs); err != nil {
		t.Fatalf("open JSON: %v", err)
	}
	if hs.SID == "" {
		t.Fatal("ws-only handshake sid is empty")
	}
	if len(hs.Upgrades) != 0 {
		t.Errorf("ws-only upgrades = %v, want none", hs.Upgrades)
	}

	writeText(t, c, "4hello ws")
	mt, text, _ = readFrame(t, c)
	if text != "4hello ws" {
		t.Errorf("echo = %q, want %q", text, "4hello ws")
	}

	writeText(t, c, "2beat")
	_, text, _ = readFrame(t, c)
	if text != "1" {
		t.Errorf("client ping response %q, want protocol close 1", text)
	}
}

func TestWebSocketBinaryEcho(t *testing.T) {
	h := newHarness(t, nil)
	c := h.dialWS("EIO=4&transport=websocket")
	readFrame(t, c) // open

	payload := []byte{0x00, 0xff, 0x1e, 0x62}
	if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("binary write: %v", err)
	}
	mt, _, data := readFrame(t, c)
	if mt != websocket.BinaryMessage {
		t.Fatalf("echo frame type = %v, want binary", mt)
	}
	if !bytes.Equal(data, payload) {
		t.Errorf("binary echo = %x, want %x", data, payload)
	}
	select {
	case got := <-h.blobs:
		if !bytes.Equal(got, payload) {
			t.Errorf("OnMessage blob = %x, want %x", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessage not called for binary frame")
	}
}

func TestUpgradeFromPolling(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	pollResult := make(chan string, 1)
	go func() {
		pollResult <- h.pollOnce(sid)
	}()
	time.Sleep(50 * time.Millisecond) // let the poll park

	c := h.dialWS("EIO=4&transport=websocket&sid=" + sid)

	// probe → pong
	writeText(t, c, "2probe")
	if _, text, _ := readFrame(t, c); text != "3probe" {
		t.Fatalf("probe answered %q, want %q", text, "3probe")
	}

	// accept: the old poll must retire with a noop
	writeText(t, c, "5")
	select {
	case body := <-pollResult:
		if body != "6" {
			t.Errorf("old poll after upgrade = %q, want noop 6", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old poll never retired after upgrade")
	}

	// application traffic now flows on the socket
	writeText(t, c, "4over-ws")
	if _, text, _ := readFrame(t, c); text != "4over-ws" {
		t.Errorf("post-upgrade echo = %q", text)
	}

	// A client pong is accepted after the upgrade (it answers the server's
	// ping), and the session keeps working afterwards.
	writeText(t, c, "3")
	writeText(t, c, "4after-pong")
	if _, text, _ := readFrame(t, c); text != "4after-pong" {
		t.Errorf("echo after pong = %q", text)
	}

	// polling after the switch is a protocol violation and closes the session
	resp, err := h.get(sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("poll after upgrade status = %d, want 400", resp.StatusCode)
	}
}

func TestFailedProbeKeepsPollingSession(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	// Open a probe and slam it shut before the upgrade packet.
	c := h.dialWS("EIO=4&transport=websocket&sid=" + sid)
	writeText(t, c, "2probe")
	if _, text, _ := readFrame(t, c); text != "3probe" {
		t.Fatalf("probe answered %q", text)
	}
	_ = c.Close()

	// The session must still work over polling.
	if resp := h.post(sid, "4still-here"); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST after failed probe status = %d", resp.StatusCode)
	}
	select {
	case msg := <-h.messages:
		if msg != "still-here" {
			t.Errorf("message = %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session died after a failed probe")
	}
}

func TestMessageBeforeUpgradeIsProtocolError(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID

	c := h.dialWS("EIO=4&transport=websocket&sid=" + sid)
	writeText(t, c, "4too-early")

	// The server closes the socket in response.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	select {
	case reason := <-h.closed:
		if reason != CloseProtocol {
			t.Errorf("close reason = %q, want %q", reason, CloseProtocol)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed after early message")
	}
}

func TestWebSocketUnknownSIDCloses(t *testing.T) {
	h := newHarness(t, nil)
	wsURL := "ws" + strings.TrimPrefix(h.ts.URL, "http") + "/engine.io/?EIO=4&transport=websocket&sid=bogus"
	// An unknown sid is refused at the HTTP layer, before any WebSocket
	// session could exist.
	if _, _, err := websocket.DefaultDialer.Dial(wsURL, nil); err == nil {
		t.Fatal("ws dial with unknown sid unexpectedly succeeded")
	}
}
