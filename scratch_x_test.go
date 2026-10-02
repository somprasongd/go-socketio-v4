package socketio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestScratchRecoveryIdentity(t *testing.T) {
	srv := newServer()
	ns := srv.DefaultNamespace()
	ns.EnableRecovery(nil)
	f1 := newFakeSink(srv)

	reply := connectWithAuth(t, f1, "")
	var payload map[string]any
	_ = json.Unmarshal([]byte(strings.TrimPrefix(reply, "0")), &payload)
	pid := payload["pid"].(string)

	ns.OnEvent("join", func(s *Socket, _ []any, _ func(...any)) {
		s.Join("readers")
	})
	f1.clientSends(`2["join"]`)
	time.Sleep(100 * time.Millisecond)

	s1 := ns.FetchSockets()[0]
	st := ns.recoveryFields()
	e := st.lookup(pid)
	t.Logf("s.rec == lookup(pid)? %v", s1.rec == e)
	t.Logf("s.rec pid=%v lookup pid=%v", s1.rec.pid, e.pid)
	st.mu.Lock()
	t.Logf("entry count=%d", len(st.entries))
	for p, ent := range st.entries {
		t.Logf("  entry %p pid=%s socket=%s", ent, p, ent.socketID)
	}
	st.mu.Unlock()
}
