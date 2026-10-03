package engineio

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestEmptyProbeFrameClosesSessionWithoutPanic(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID
	c := h.dialWS("EIO=4&transport=websocket&sid=" + sid)
	defer c.Close()
	writeText(t, c, "")
	select {
	case reason := <-h.closed:
		if reason != CloseProtocol {
			t.Fatal(reason)
		}
	case <-time.After(time.Second):
		t.Fatal("empty probe did not close")
	}
}

func TestWebSocketInboundMaxPayload(t *testing.T) {
	for _, binary := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "binary"}[binary], func(t *testing.T) {
			h := newHarness(t, &Options{MaxPayload: 64})
			c := h.dialWS("EIO=4&transport=websocket")
			defer c.Close()
			readFrame(t, c)
			mt := websocket.TextMessage
			data := []byte("4" + strings.Repeat("x", 4096))
			if binary {
				mt = websocket.BinaryMessage
				data = data[1:]
			}
			if err := c.WriteMessage(mt, data); err != nil {
				t.Fatal(err)
			}
			select {
			case <-h.closed:
			case <-time.After(time.Second):
				t.Fatal("oversized frame kept session alive")
			}
			select {
			case <-h.messages:
				t.Fatal("oversized message dispatched")
			case <-h.blobs:
				t.Fatal("oversized binary dispatched")
			default:
			}
		})
	}
}

func TestWebSocketWriteDeadlineClosesSlowReader(t *testing.T) {
	srv := NewServer(&Options{WriteTimeout: 40 * time.Millisecond})
	sessions := make(chan *Session, 1)
	srv.OnSession = func(s *Session) { sessions <- s }
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer srv.Close()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if tcp, ok := c.UnderlyingConn().(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	_, _, _ = c.ReadMessage()
	s := <-sessions
	finished := make(chan error, 1)
	go func() {
		for range 64 {
			if err := s.SendBinary(make([]byte, 512*1024)); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("slow reader did not reach bounded write failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write deadline did not unblock sender")
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("write failure left session alive")
	}
	closed := make(chan struct{})
	go func() { srv.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
}

func TestOnSessionPrecedesMessageDispatch(t *testing.T) {
	for _, transport := range []string{"polling", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			srv := NewServer(nil)
			ts := httptest.NewServer(srv)
			defer ts.Close()
			defer srv.Close()
			entered := make(chan *Session, 1)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			dispatched := make(chan struct{}, 1)
			srv.OnSession = func(s *Session) { entered <- s; <-release }
			srv.OnMessage = func(*Session, []byte, bool) { dispatched <- struct{}{} }
			if transport == "websocket" {
				c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/?EIO=4&transport=websocket", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				_, _, _ = c.ReadMessage()
				<-entered
				if err = c.WriteMessage(websocket.TextMessage, []byte("4first")); err != nil {
					t.Fatal(err)
				}
			} else {
				handshakeDone := make(chan struct{})
				go func() {
					defer close(handshakeDone)
					res, err := ts.Client().Get(ts.URL + "/?EIO=4&transport=polling")
					if err == nil {
						_, _ = io.Copy(io.Discard, res.Body)
						res.Body.Close()
					}
				}()
				s := <-entered
				res, err := ts.Client().Post(ts.URL+"/?EIO=4&transport=polling&sid="+s.ID(), "text/plain", strings.NewReader("4first"))
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				defer func() { unblock(); <-handshakeDone }()
			}
			select {
			case <-dispatched:
				t.Fatal("message dispatched before OnSession returned")
			case <-time.After(40 * time.Millisecond):
			}
			unblock()
			select {
			case <-dispatched:
			case <-time.After(time.Second):
				t.Fatal("first queued message lost")
			}
		})
	}
}

func TestApplicationTrafficDoesNotSuppressPingsOrReplacePongs(t *testing.T) {
	for _, answer := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-pong", true: "answered"}[answer], func(t *testing.T) {
			srv := NewServer(&Options{PingInterval: 30 * time.Millisecond, PingTimeout: 40 * time.Millisecond})
			srv.OnMessage = func(*Session, []byte, bool) {}
			closed := make(chan CloseReason, 1)
			srv.OnClose = func(_ *Session, r CloseReason) { closed <- r }
			ts := httptest.NewServer(srv)
			defer ts.Close()
			defer srv.Close()
			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/?EIO=4&transport=websocket", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_, _, _ = c.ReadMessage()
			var writer sync.Mutex
			send := func(data string) {
				writer.Lock()
				defer writer.Unlock()
				_ = c.WriteMessage(websocket.TextMessage, []byte(data))
			}
			pings := make(chan struct{}, 10)
			go func() {
				for {
					_, data, err := c.ReadMessage()
					if err != nil {
						return
					}
					if string(data) == "2" {
						pings <- struct{}{}
						if answer {
							send("3")
						}
					}
				}
			}()
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						send("4traffic")
					}
				}
			}()
			if !answer {
				select {
				case reason := <-closed:
					if reason != CloseTimeout {
						t.Fatal(reason)
					}
				case <-time.After(time.Second):
					t.Fatal("application traffic replaced required pong")
				}
			} else {
				for range 3 {
					select {
					case <-pings:
					case <-time.After(time.Second):
						t.Fatal("traffic suppressed server ping")
					}
				}
				select {
				case reason := <-closed:
					t.Fatal(reason)
				default:
				}
			}
		})
	}
}

func TestFailedProbeReturnsToParkedPolling(t *testing.T) {
	h := newHarness(t, nil)
	sid := h.handshake().SID
	s := h.srv.getSession(sid)
	c := h.dialWS("EIO=4&transport=websocket&sid=" + sid)
	writeText(t, c, "2probe")
	readFrame(t, c)
	_ = c.Close()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		ready := s.probe == nil
		s.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("probe cleanup stalled")
		}
		time.Sleep(time.Millisecond)
	}
	result := make(chan string, 1)
	go func() { result <- h.pollOnce(sid) }()
	select {
	case body := <-result:
		t.Fatalf("failed probe spun polling: %q", body)
	case <-time.After(40 * time.Millisecond):
	}
	_ = s.SendText("resume")
	select {
	case body := <-result:
		if body != "4resume" {
			t.Fatal(body)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not resume")
	}
}

func TestAllowedOriginsProduceCORSAndPreflightResponses(t *testing.T) {
	for _, credentials := range []bool{false, true} {
		srv := NewServer(&Options{AllowedOrigins: []string{"https://frontend.example"}, AllowCredentials: credentials})
		for _, method := range []string{http.MethodGet, http.MethodOptions} {
			req := httptest.NewRequest(method, "http://backend/?EIO=4&transport=polling", nil)
			req.Header.Set("Origin", "https://frontend.example")
			req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)
			if w.Header().Get("Access-Control-Allow-Origin") != "https://frontend.example" || !strings.Contains(w.Header().Get("Vary"), "Origin") {
				t.Fatal(w.Header())
			}
			if (w.Header().Get("Access-Control-Allow-Credentials") == "true") != credentials {
				t.Fatal(w.Header())
			}
			if method == http.MethodOptions && (w.Code != 204 || w.Header().Get("Access-Control-Allow-Headers") != "authorization, content-type") {
				t.Fatal(w.Code, w.Header())
			}
		}
		req := httptest.NewRequest(http.MethodOptions, "http://backend/", nil)
		req.Header.Set("Origin", "https://blocked.example")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		if w.Code != 403 || w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal(w.Code, w.Header())
		}
		srv.Close()
	}
}
