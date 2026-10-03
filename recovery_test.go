package socketio

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// connectWithAuth performs a CONNECT with a raw auth payload and returns
// the server's connect reply.
func connectWithAuth(t *testing.T, f *fakeSink, auth string) string {
	t.Helper()
	f.clientSends("0" + auth)
	return f.last()
}

func TestRecoveryConnectCarriesPID(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)

	reply := connectWithAuth(t, f, "")
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload); err != nil {
		t.Fatalf("connect reply %q: %v", reply, err)
	}
	if payload["pid"] == "" {
		t.Errorf("connect reply has no pid: %q", reply)
	}
	if payload["sid"] == "" {
		t.Errorf("connect reply has no sid: %q", reply)
	}
}

func TestEventsCarryOffsets(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f := newFakeSink(srv)
	connectWithAuth(t, f, "")
	s := ns.FetchSockets()[0]

	_ = s.Emit("seed", "one")
	_ = s.Emit("seed", "two")

	sent := f.drain()
	// [0] connect reply, [1] first event, [2] second event
	if !strings.HasSuffix(sent[1], `,"1"]`) {
		t.Errorf("first event = %q, want trailing offset 1", sent[1])
	}
	if !strings.HasSuffix(sent[2], `,"2"]`) {
		t.Errorf("second event = %q, want trailing offset 2", sent[2])
	}
}

func TestRecoveryRestoresSessionAndReplays(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)

	recoveredSeen := make(chan bool, 1)
	ns.OnConnect(func(s *Socket) { recoveredSeen <- s.Recovered() })

	f1 := newFakeSink(srv)
	reply := connectWithAuth(t, f1, "")
	var payload map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload)
	pid := payload["pid"].(string)

	// the first connect is not a recovery
	select {
	case rec := <-recoveredSeen:
		if rec {
			t.Error("first connect reported recovered=true")
		}
	case <-time.After(time.Second):
		t.Fatal("OnConnect never fired for the first connect")
	}

	// join a room and receive one event (initialises the client offset)
	f1.clientSends(`2["join"]`)
	ns.OnEvent("join", func(s *Socket, _ []any, _ func(...any)) { s.Join("readers") })
	f1.clientSends(`2["join"]`)
	s1 := ns.FetchSockets()[0]
	_ = s1.Emit("seed")

	// unexpected close: the session must be held
	srv.detach(f1, reasonTransportClose)

	// events fired during the gap are buffered for the room
	ns.To("readers").Emit("missed", "during-gap")

	// reconnect with pid + the last offset the client processed
	f2 := newFakeSink(srv)
	f2.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	var reply2 string
	for _, frame := range f2.drain() {
		if strings.HasPrefix(frame, "0") {
			reply2 = frame
			break
		}
	}
	if reply2 == "" {
		t.Fatal("recovery reply never arrived")
	}
	var payload2 map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply2, "0")), &payload2)
	if payload2["sid"] != payload["sid"] {
		t.Errorf("recovered sid = %v, want the original %v", payload2["sid"], payload["sid"])
	}
	if payload2["pid"] != pid {
		t.Errorf("recovered pid = %v, want %v", payload2["pid"], pid)
	}

	// the missed event replays right after the connect reply
	replay := f2.drain()
	found := false
	for _, piece := range replay {
		if strings.Contains(piece, `"missed"`) {
			if !strings.HasSuffix(piece, `,"2"]`) {
				t.Errorf("replay = %q, want offset 2", piece)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("replay never arrived; wire = %v", replay)
	}

	select {
	case rec := <-recoveredSeen:
		if !rec {
			t.Error("OnConnect fired with recovered=false")
		}
	case <-time.After(time.Second):
		t.Fatal("OnConnect never fired for the recovered session")
	}

	// the room came back with the session: a room broadcast reaches it
	ns.To("readers").Emit("room-check")
	if last := f2.last(); !strings.Contains(last, `"room-check"`) {
		t.Errorf("room restored check = %q", last)
	}
}

func TestRecoveryWindowExpiry(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(&RecoveryOptions{MaxDisconnectionDuration: 30 * time.Millisecond})
	f1 := newFakeSink(srv)

	reply := connectWithAuth(t, f1, "")
	var payload map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload)
	pid := payload["pid"].(string)
	oldSID := payload["sid"].(string)

	srv.detach(f1, reasonPingTimeout)
	time.Sleep(60 * time.Millisecond)

	f2 := newFakeSink(srv)
	f2.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	reply2 := f2.last()
	if strings.Contains(reply2, `"sid":"`+oldSID+`"`) {
		t.Errorf("expired session was recovered: %q vs old %q", reply2, oldSID)
	}
}

func TestDeliberateDisconnectsDoNotRecover(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f1 := newFakeSink(srv)

	reply := connectWithAuth(t, f1, "")
	var payload map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload)
	pid := payload["pid"].(string)
	oldSID := payload["sid"].(string)

	// the client says goodbye → no recovery
	f1.clientSends("1")

	f2 := newFakeSink(srv)
	f2.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	reply2 := f2.last()
	if strings.Contains(reply2, `"sid":"`+oldSID+`"`) {
		t.Errorf("deliberate disconnect was recovered: %q", reply2)
	}
}

func TestRecoverySkipMiddlewaresOption(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	sawRecovered := 0
	ns.Use(func(s *Socket) error {
		if s.Recovered() {
			sawRecovered++
			return errors.New("blocked users must not ride recovery past me")
		}
		return nil
	})
	ns.EnableRecovery(&RecoveryOptions{SkipMiddlewares: true})
	f1 := newFakeSink(srv)

	reply := connectWithAuth(t, f1, "")
	var payload map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload)
	pid := payload["pid"].(string)

	_ = ns.FetchSockets()[0].Emit("seed")
	srv.detach(f1, reasonTransportClose)

	f2 := newFakeSink(srv)
	f2.clientSends(`0{"pid":"` + pid + `","offset":"1"}`)
	var reply2 string
	for _, frame := range f2.drain() {
		if strings.HasPrefix(frame, "0") {
			reply2 = frame
			break
		}
	}
	if !strings.Contains(reply2, payload["sid"].(string)) {
		t.Errorf("recovery failed despite SkipMiddlewares: %q", reply2)
	}
	if sawRecovered != 0 {
		t.Errorf("middleware ran %d time(s) on recovery despite SkipMiddlewares", sawRecovered)
	}
}

func TestNoRecoveryWithoutEnable(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	reply := connectWithAuth(t, f, "")
	if strings.Contains(reply, "pid") {
		t.Errorf("plain server leaked a pid into the connect reply: %q", reply)
	}
}
