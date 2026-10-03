package redisadapter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	socketio "github.com/somprasongd/go-socketio-v4"
)

// instance is one simulated server process: its own socketio server, its
// own adapter, all pointed at the same Redis.
type instance struct {
	srv *socketio.Server
	ts  *httptest.Server
	ad  *Adapter
}

func startInstance(t *testing.T, rdb redis.UniversalClient, nsp string) *instance {
	t.Helper()
	srv := socketio.New(nil)
	ns := srv.Of(nsp)
	ad := New(nsp, rdb, "go-socketio-relay")
	if err := ad.Ready(); err != nil {
		t.Fatal(err)
	}
	ns.SetAdapter(ad)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(func() { _ = ad.Close() })
	return &instance{srv: srv, ts: ts, ad: ad}
}

// wsConnect opens a raw socket.io client on an instance (websocket-only).
func wsConnect(t *testing.T, in *instance, nsp string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(in.ts.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	readText(t, c) // engine.io OPEN "0{...}"
	// a non-default namespace CONNECT carries the wire syntax "/admin,"
	connect := "40"
	if nsp != "" {
		connect += nsp + ","
	}
	sendText(t, c, connect)
	for {
		frame := readText(t, c)
		if strings.HasPrefix(frame, "40") {
			break // namespace connected
		}
	}
	return c
}

func sendText(t *testing.T, c *websocket.Conn, s string) {
	t.Helper()
	if err := c.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
		t.Fatalf("write %q: %v", s, err)
	}
}

func readText(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		mt, data, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if mt == websocket.TextMessage {
			return string(data)
		}
	}
}

// readUntil scans frames for one containing substr, within a deadline.
func readUntil(t *testing.T, c *websocket.Conn, substr string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		mt, data, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read while waiting for %q: %v", substr, err)
		}
		if mt == websocket.TextMessage && strings.Contains(string(data), substr) {
			return string(data)
		}
	}
	t.Fatalf("frame containing %q never arrived", substr)
	return ""
}

// readForbiddenFrame consumes the whole observation window, rather than
// treating the first unrelated frame as proof that a broadcast was excluded.
// A timeout ends the Gorilla reader; these negative checks are the client's
// final operation and callers must not reuse the connection afterward.
func readForbiddenFrame(c *websocket.Conn, substr string, window time.Duration) (bool, error) {
	if err := c.SetReadDeadline(time.Now().Add(window)); err != nil {
		return false, err
	}
	for {
		mt, data, err := c.ReadMessage()
		if err != nil {
			if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
				return false, nil
			}
			return false, err
		}
		if mt == websocket.TextMessage && strings.Contains(string(data), substr) {
			return true, nil
		}
	}
}

func assertNoFrameWith(t *testing.T, c *websocket.Conn, substr string, window time.Duration) {
	t.Helper()
	found, err := readForbiddenFrame(c, substr, window)
	if err != nil {
		t.Fatalf("reading exclusion window: %v", err)
	}
	if found {
		t.Fatalf("unexpected frame containing %q", substr)
	}
}

func TestBroadcastCrossesProcesses(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	nodeA := startInstance(t, rdb, "/")
	nodeB := startInstance(t, rdb, "/")

	clientA := wsConnect(t, nodeA, "")
	clientB := wsConnect(t, nodeB, "")

	// a broadcast from process A reaches the client connected to process B
	nsA := nodeA.srv.DefaultNamespace()
	nsA.Emit("from-a", "hello across")
	if frame := readUntil(t, clientB, "from-a"); !strings.Contains(frame, `"hello across"`) {
		t.Errorf("clientB frame = %q", frame)
	}

	// and the local client of A still receives its own instance's emit
	if frame := readUntil(t, clientA, "from-a"); !strings.Contains(frame, `"hello across"`) {
		t.Errorf("clientA frame = %q", frame)
	}
}

func TestRoomScopingAcrossProcesses(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	nodeA := startInstance(t, rdb, "/")
	nodeB := startInstance(t, rdb, "/")
	nodeC := startInstance(t, rdb, "/")

	// register the join handler before any client speaks
	nodeB.srv.DefaultNamespace().OnEvent("join", func(s *socketio.Socket, _ []any, _ func(...any)) {
		s.Join("readers")
	})

	clientA := wsConnect(t, nodeA, "")
	clientB := wsConnect(t, nodeB, "")
	clientC := wsConnect(t, nodeC, "")

	// B joins "readers"; A and C never do
	sendText(t, clientB, `42["join"]`)
	time.Sleep(200 * time.Millisecond)

	nodeA.srv.DefaultNamespace().To("readers").Emit("room-msg", "only members")

	// B (in the room, on another process) gets it…
	if frame := readUntil(t, clientB, "room-msg"); !strings.Contains(frame, `"only members"`) {
		t.Errorf("clientB frame = %q", frame)
	}
	// …while A (same process as the broadcaster) and C (third process),
	// neither in the room, stay silent.
	assertNoFrameWith(t, clientA, "room-msg", 600*time.Millisecond)
	assertNoFrameWith(t, clientC, "room-msg", 600*time.Millisecond)
}

func TestBinarySurvivesRelay(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	nodeA := startInstance(t, rdb, "/")
	nodeB := startInstance(t, rdb, "/")
	clientB := wsConnect(t, nodeB, "")

	payload := []byte{0x00, 0xff, 0x7e, 0x1e}
	nodeA.srv.DefaultNamespace().Emit("blob", payload)

	// binary arrives as text placeholder + binary frame
	textFrame := readText(t, clientB)
	if !strings.HasPrefix(textFrame, `451-["blob",{"_placeholder":true,"num":0}]`) {
		t.Fatalf("relay text = %q", textFrame)
	}
	_ = clientB.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, data, err := clientB.ReadMessage()
	if err != nil {
		t.Fatalf("binary read: %v", err)
	}
	if mt != websocket.BinaryMessage || !bytes.Equal(data, payload) {
		t.Fatalf("relay binary = (%v, %x)", mt, data)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	nodeA := startInstance(t, rdb, "/")
	nodeB := startInstance(t, rdb, "/admin")

	clientA := wsConnect(t, nodeA, "")
	clientB := wsConnect(t, nodeB, "/admin")

	// a "/" broadcast must not leak into the /admin client
	nodeA.srv.DefaultNamespace().Emit("root-only", 1)
	assertNoFrameWith(t, clientB, "root-only", 600*time.Millisecond)
	_ = clientA
}

func TestVolatileRelayToSocketRoom(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	a := startInstance(t, rdb, "/")
	b := startInstance(t, rdb, "/")
	ready := wsConnect(t, b, "")
	// A polling connection without a parked GET is intentionally unwritable.
	response, err := http.Get(b.ts.URL + "/socket.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	var open map[string]any
	if err := json.Unmarshal(body[1:], &open); err != nil {
		t.Fatal(err)
	}
	url := b.ts.URL + "/socket.io/?EIO=4&transport=polling&sid=" + open["sid"].(string)
	post, err := http.Post(url, "text/plain", strings.NewReader("40"))
	if err != nil {
		t.Fatal(err)
	}
	_ = post.Body.Close()
	get, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(get.Body)
	_ = get.Body.Close()
	nsA := a.srv.DefaultNamespace()
	nsA.Volatile().Emit("volatile", 1)
	nsA.Emit("barrier")
	// Receive the barrier on websocket first: pub/sub order proves the volatile
	// message was applied while the polling client had no GET parked.
	readUntil(t, ready, "volatile")
	readUntil(t, ready, "barrier")
	get, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(get.Body)
	_ = get.Body.Close()
	if strings.Contains(string(body), "volatile") || !strings.Contains(string(body), "barrier") {
		t.Fatalf("poll=%s", body)
	}
	var target string
	// All sockets have their own ID room; a targeted volatile goes through the
	// same relay routing as any room. Use a fresh websocket with a known handler.
	connected := make(chan string, 1)
	b.srv.DefaultNamespace().OnConnect(func(s *socketio.Socket) { connected <- s.ID() })
	single := wsConnect(t, b, "")
	target = <-connected
	nsA.To(target).Volatile().Emit("target-only")
	readUntil(t, single, "target-only")
	nsA.Emit("target-barrier")
	frame := readText(t, ready)
	if !strings.Contains(frame, "target-barrier") {
		t.Fatal("target leaked")
	}
}

func TestRemoteBroadcastBuffersHeldSession(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	a := startInstance(t, rdb, "/")
	b := startInstance(t, rdb, "/")
	nsB := b.srv.DefaultNamespace()
	nsB.EnableRecovery(nil)
	connected := make(chan *socketio.Socket, 2)
	disconnected := make(chan struct{}, 1)
	nsB.OnConnect(func(s *socketio.Socket) { connected <- s })
	nsB.OnDisconnect(func(*socketio.Socket, string) { disconnected <- struct{}{} })
	url := "ws" + strings.TrimPrefix(b.ts.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	readText(t, c)
	sendText(t, c, "40")
	reply := readUntil(t, c, "40")
	var identity map[string]any
	if err := json.Unmarshal([]byte(reply[2:]), &identity); err != nil {
		t.Fatal(err)
	}
	s := <-connected
	_ = s.Emit("seed")
	readUntil(t, c, "seed")
	_ = c.Close()
	<-disconnected
	// Observe the remote receive path on a live B client, so recovery begins
	// only after B has processed the held-session broadcast.
	witness := wsConnect(t, b, "")
	<-connected
	a.srv.DefaultNamespace().Emit("missed-remote")
	readUntil(t, witness, "missed-remote")
	next, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close() })
	readText(t, next)
	sendText(t, next, `40{"pid":"`+identity["pid"].(string)+`","offset":"1"}`)
	restoredReply := readUntil(t, next, "40")
	restored := <-connected
	if !restored.Recovered() || !strings.Contains(restoredReply, identity["sid"].(string)) {
		t.Fatalf("not restored: %s", restoredReply)
	}
	readUntil(t, next, "missed-remote")
	a.srv.DefaultNamespace().Emit("after-replay")
	frame := readText(t, next)
	if !strings.Contains(frame, "after-replay") {
		t.Fatalf("duplicate replay: %s", frame)
	}
}

func TestNegativeWindowDetectsForbiddenFrameAfterUnrelatedFrame(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	in := startInstance(t, rdb, "/")
	c := wsConnect(t, in, "")
	in.srv.DefaultNamespace().Emit("barrier")
	in.srv.DefaultNamespace().Emit("secret-leak")
	found, err := readForbiddenFrame(c, "secret-leak", time.Second)
	if err != nil || !found {
		t.Fatalf("late forbidden frame missed: found=%v err=%v", found, err)
	}
}

func TestTypedBinarySurvivesPubSubEncoding(t *testing.T) {
	args, err := encodeArgs([]any{map[string][]byte{"data": {1, 2, 3}}, [][]byte{{4}, {5}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	if data, ok := decoded[0].(map[string]any)["data"].([]byte); !ok || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Fatal(decoded)
	}
	chunks := decoded[1].([]any)
	if !bytes.Equal(chunks[0].([]byte), []byte{4}) || !bytes.Equal(chunks[1].([]byte), []byte{5}) {
		t.Fatal(decoded)
	}
}
