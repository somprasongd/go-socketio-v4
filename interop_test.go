package socketio

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestJSInterop runs the real socket.io v4 JavaScript client against the Go
// server (interop/interop.mjs). It skips, with a reason, when node or the
// client dependency is unavailable.
func TestJSInterop(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the JS interop suite")
	}
	dir, err := filepath.Abs("interop")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "node_modules")); os.IsNotExist(err) {
		npm, err := exec.LookPath("npm")
		if err != nil {
			t.Skip("npm is not installed; cannot fetch socket.io-client")
		}
		install := exec.Command(npm, "ci")
		install.Dir = dir
		if out, err := install.CombinedOutput(); err != nil {
			t.Skipf("npm ci failed (%v): %s", err, out)
		}
	}

	srv := New(nil)
	ns := srv.DefaultNamespace()
	ns.OnEvent("echo", func(s *Socket, args []any, ack func(response ...any)) {
		if ack != nil && len(args) > 0 {
			ack(args[0])
		}
	})
	ns.OnEvent("ask", func(s *Socket, args []any, ack func(response ...any)) {
		// The server asks the client, the client answers 42, and the server
		// reports the answer back as its own event — making the whole acked
		// emit path observable from the client.
		go func() {
			select {
			case res := <-s.EmitWithAck("provide"):
				s.Emit("provide-result", res...)
			case <-time.After(2 * time.Second):
				s.Emit("provide-result", "timeout")
			}
		}()
	})
	ns.OnEvent("bin-up", func(s *Socket, args []any, ack func(response ...any)) {
		if blob, ok := args[0].([]byte); ok {
			s.Emit("bin-down", blob)
		}
	})
	ns.OnEvent("join-room", func(s *Socket, args []any, ack func(response ...any)) {
		s.Join("room")
	})
	ns.OnEvent("to-room", func(s *Socket, args []any, ack func(response ...any)) {
		ns.To("room").Emit("room-msg", "hello room")
	})
	ns.OnEvent("to-room-except-me", func(s *Socket, args []any, ack func(response ...any)) {
		s.To("room").Emit("exclusive", "just you")
	})
	ns.OnEvent("kick-me", func(s *Socket, args []any, ack func(response ...any)) {
		s.Disconnect()
	})
	srv.Of("/admin").OnConnect(func(s *Socket) {
		s.Emit("welcome", "admin here")
	})

	// A second server enforces credentials through connection middleware —
	// the auth path must be observable from the real client.
	authSrv := New(nil)
	authNs := authSrv.DefaultNamespace()
	authNs.Use(func(s *Socket) error {
		if s.Handshake()["token"] != "sekrit" {
			return errors.New("invalid credentials")
		}
		return nil
	})
	authTS := httptest.NewServer(authSrv)
	defer authTS.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "interop.mjs")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "INTEROP_URL="+ts.URL, "INTEROP_AUTH_URL="+authTS.URL)
	out, err := cmd.CombinedOutput()
	t.Logf("interop output:\n%s", out)
	if err != nil {
		t.Fatalf("interop suite failed: %v", err)
	}
}
