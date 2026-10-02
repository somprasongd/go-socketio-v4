package socketio

import (
	"strings"
	"testing"
)

func TestVolatileDropsWhenNotWritable(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	sid := connectClient(t, f)
	s := srv.DefaultNamespace().FetchSockets()[0]

	// a polling client between GETs cannot receive anything right now
	f.mu.Lock()
	f.writable = false
	f.mu.Unlock()
	before := len(f.drain())

	if err := s.Volatile().Emit("tick", 1); err != nil {
		t.Fatalf("volatile emit errored: %v", err)
	}
	if got := len(f.drain()); got != before {
		t.Errorf("volatile emit was buffered (%d pieces after), want dropped", got)
	}

	// the same event without the flag must still be buffered — volatile
	// never changes what plain Emit does
	if err := s.Emit("tick", 2); err != nil {
		t.Fatalf("plain emit errored: %v", err)
	}
	if got := len(f.drain()); got != before+1 {
		t.Errorf("plain emit missing after %d pieces", got)
	}
	if last := f.last(); !strings.Contains(last, "2") {
		t.Errorf("plain emit wire = %q", last)
	}
	_ = sid
}

func TestVolatileSendsWhenWritable(t *testing.T) {
	srv := newServer()
	f := newFakeSink(srv)
	connectClient(t, f)
	s := srv.DefaultNamespace().FetchSockets()[0]

	if err := s.Volatile().Emit("tick", 1); err != nil {
		t.Fatalf("volatile emit errored: %v", err)
	}
	if !strings.Contains(f.last(), `["tick",1]`) {
		t.Errorf("wire = %q, want the volatile event delivered", f.last())
	}
}

func TestVolatileBroadcastSkipsOnlyUnwritableRecipients(t *testing.T) {
	srv := newServer()
	fReady := newFakeSink(srv)
	fBusy := newFakeSink(srv)
	connectClient(t, fReady)
	connectClient(t, fBusy)

	fBusy.mu.Lock()
	fBusy.writable = false
	fBusy.mu.Unlock()

	srv.DefaultNamespace().Volatile().Emit("tick", 1)

	if !strings.Contains(strings.Join(fReady.drain(), "\n"), `["tick",1]`) {
		t.Error("writable recipient missed the volatile broadcast")
	}
	for _, piece := range fBusy.drain() {
		if strings.Contains(piece, `["tick",1]`) {
			t.Error("unwritable recipient received the volatile broadcast")
		}
	}
	// the busy recipient still gets plain broadcasts
	srv.DefaultNamespace().Emit("tick", 2)
	if !strings.Contains(strings.Join(fBusy.drain(), "\n"), `["tick",2]`) {
		t.Error("unwritable recipient missed the plain broadcast")
	}
}
