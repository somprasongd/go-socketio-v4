package redisstreamsadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/somprasongd/go-socketio-v4/parser"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	socketio "github.com/somprasongd/go-socketio-v4"
)

// TestRedisProcessHelper runs only as an isolated child of the acceptance test.
func TestRedisProcessHelper(t *testing.T) {
	if os.Getenv("SOCKETIO_PROCESS_HELPER") != "1" {
		return
	}
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("SOCKETIO_REDIS_ADDR"), ContextTimeoutEnabled: true})
	defer rdb.Close()
	srv := socketio.New(nil)
	adapters := map[string]*Adapter{}
	var disconnected atomic.Int64
	for _, name := range []string{"/", "/admin"} {
		ns := srv.Of(name)
		ad, err := New(context.Background(), name, rdb, &Options{Prefix: os.Getenv("SOCKETIO_TEST_PREFIX"), BlockTime: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		defer ad.Close()
		adapters[name] = ad
		ns.SetAdapter(ad)
		ns.EnableRecovery(nil)
		ns.Use(func(s *socketio.Socket) error {
			if !s.Recovered() {
				return nil
			}
			if s.Handshake()["rejectRecovery"] == true {
				return errors.New("blocked recovery")
			}
			if s.Handshake()["duringRestore"] == true {
				record, err := socketio.NewRecoveryRecord(name, nil, nil, "during-restore", nil, false)
				record.SocketIDs = []string{s.ID()}
				if err == nil {
					err = ad.PublishRecord(record)
				}
				return err
			}
			return nil
		})
		ns.OnConnect(func(s *socketio.Socket) {
			if !s.Recovered() {
				s.SetData(map[string]any{"user": "synthetic"})
				s.Join("room")
			}
			_ = s.Emit("state", map[string]any{"recovered": s.Recovered(), "data": s.GetData(), "rooms": s.Rooms()})
		})
		ns.OnEvent("leave-own", func(s *socketio.Socket, _ []any, _ func(...any)) { s.Leave(s.ID()); _ = s.Emit("left-own") })
		ns.OnDisconnect(func(*socketio.Socket, string) { disconnected.Add(1) })
	}
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"disconnected": disconnected.Load()})
	})
	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		var request struct {
			Namespace string
			Rooms     []string
			Except    map[string]struct{}
			Event     string
			Args      []any
			Binary    []byte
			Volatile  bool
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		ad := adapters[request.Namespace]
		if ad == nil {
			http.Error(w, "namespace", 400)
			return
		}
		if request.Binary != nil {
			request.Args = append(request.Args, request.Binary)
		}
		record, err := socketio.NewRecoveryRecord(request.Namespace, request.Rooms, request.Except, request.Event, request.Args, request.Volatile)
		if err == nil {
			err = ad.PublishRecord(record)
		}
		if err != nil {
			http.Error(w, "publish failed", 503)
			return
		}
		w.WriteHeader(204)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("SOCKETIO_URL=http://" + ln.Addr().String())
	if err = http.Serve(ln, mux); err != nil {
		t.Fatal(err)
	}
}

func startProcess(t *testing.T, prefix string) (string, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRedisProcessHelper$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "SOCKETIO_PROCESS_HELPER=1", "SOCKETIO_TEST_PREFIX="+prefix)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
	t.Cleanup(stop)
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "SOCKETIO_URL=") {
				ready <- strings.TrimPrefix(line, "SOCKETIO_URL=")
			}
		}
	}()
	select {
	case url := <-ready:
		return url, stop
	case <-time.After(15 * time.Second):
		t.Fatal("server process did not become ready")
		return "", stop
	}
}

// TestDistributedJSRecovery uses a real Redis and three separate Go server
// processes. Set SOCKETIO_REDIS_ADDR to an owned disposable Redis endpoint.
func TestDistributedJSRecovery(t *testing.T) {
	addr := os.Getenv("SOCKETIO_REDIS_ADDR")
	if addr == "" {
		t.Skip("set SOCKETIO_REDIS_ADDR to a disposable Redis 7.2+ endpoint")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("node is required for distributed acceptance")
	}
	dir, err := filepath.Abs("../interop")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(dir, "node_modules/socket.io-client")); err != nil {
		t.Fatal("run npm ci in interop before distributed acceptance")
	}
	prefix := fmt.Sprintf("test-distributed-%d", time.Now().UnixNano())
	a, _ := startProcess(t, prefix)
	b, stopB := startProcess(t, prefix)
	c, _ := startProcess(t, prefix)
	urls := []string{a, b, c}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "distributed.mjs")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DISTRIBUTED_A="+urls[0], "DISTRIBUTED_B="+urls[1], "DISTRIBUTED_C="+urls[2])
	out, err := cmd.CombinedOutput()
	t.Logf("distributed JS output:\n%s", out)
	if err != nil {
		t.Fatalf("distributed acceptance: %v", err)
	}
	testPersistedRestart(t, prefix, a, b, stopB)
}

func readSocketPacket(t *testing.T, c *websocket.Conn) parser.Packet {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		kind, data, err := c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if kind != websocket.TextMessage {
			t.Fatalf("unexpected binary frame")
		}
		if string(data) == "2" {
			_ = c.WriteMessage(websocket.TextMessage, []byte("3"))
			continue
		}
		if len(data) == 0 || data[0] != '4' {
			t.Fatalf("unexpected frame %s", data)
		}
		p, err := parser.Decode(string(data[1:]), nil)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
}
func rawConnect(t *testing.T, url, auth string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/socket.io/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.WriteMessage(websocket.TextMessage, []byte("40"+auth)); err != nil {
		t.Fatal(err)
	}
	return c
}
func disconnectCount(t *testing.T, url string) int64 {
	t.Helper()
	response, err := http.Get(url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var status struct{ Disconnected int64 }
	if err = json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status.Disconnected
}
func publishHTTP(t *testing.T, url, event string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"namespace": "/", "event": event})
	response, err := http.Post(url+"/publish", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatal(response.Status)
	}
}
func testPersistedRestart(t *testing.T, prefix, a, b string, stopB func()) {
	before := disconnectCount(t, b)
	c := rawConnect(t, b, "")
	reply := readSocketPacket(t, c).Data.(map[string]any)
	seed := readSocketPacket(t, c).Args()
	offset := seed[len(seed)-1].(string)
	_ = c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for disconnectCount(t, b) <= before {
		if time.Now().After(deadline) {
			t.Fatal("snapshot not saved")
		}
		time.Sleep(10 * time.Millisecond)
	}
	publishHTTP(t, a, "during-restart")
	stopB()
	restarted, _ := startProcess(t, prefix)
	auth, _ := json.Marshal(map[string]any{"pid": reply["pid"], "offset": offset})
	next := rawConnect(t, restarted, string(auth))
	restored := readSocketPacket(t, next).Data.(map[string]any)
	if restored["sid"] != reply["sid"] || restored["pid"] != reply["pid"] {
		t.Fatalf("identity lost after restart: %v", restored)
	}
	if packet := readSocketPacket(t, next); packet.Args()[0] != "during-restart" {
		t.Fatal(packet)
	}
	state := readSocketPacket(t, next).Args()[1].(map[string]any)
	if state["recovered"] != true {
		t.Fatal(state)
	}
	t.Log("PASS persisted session restored after terminating and restarting its original Go process")
}

// tcpProxy interrupts only this adapter's Redis connections, without modifying
// the disposable Redis server or other instances.
type tcpProxy struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	paused      bool
	connections map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{listener: ln, target: target, connections: map[net.Conn]struct{}{}}
	t.Cleanup(func() { _ = ln.Close(); p.pause(true) })
	go func() {
		for {
			local, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				remote, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					_ = local.Close()
					return
				}
				p.mu.Lock()
				if p.paused {
					p.mu.Unlock()
					_ = local.Close()
					_ = remote.Close()
					return
				}
				p.connections[local] = struct{}{}
				p.connections[remote] = struct{}{}
				p.mu.Unlock()
				defer func() {
					_ = local.Close()
					_ = remote.Close()
					p.mu.Lock()
					delete(p.connections, local)
					delete(p.connections, remote)
					p.mu.Unlock()
				}()
				go func() { _, _ = io.Copy(remote, local); _ = remote.Close() }()
				_, _ = io.Copy(local, remote)
			}()
		}
	}()
	return p
}
func (p *tcpProxy) pause(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = paused
	if paused {
		for c := range p.connections {
			_ = c.Close()
		}
	}
}

func TestRealRedisDisconnectResumeAndFailClosed(t *testing.T) {
	addr := os.Getenv("SOCKETIO_REDIS_ADDR")
	if addr == "" {
		t.Skip("set SOCKETIO_REDIS_ADDR to disposable Redis")
	}
	proxy := newProxy(t, addr)
	client := redis.NewClient(&redis.Options{Addr: proxy.listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true})
	direct := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close(); _ = direct.Close() })
	prefix := fmt.Sprintf("test-resume-%d", time.Now().UnixNano())
	failures := make(chan error, 10)
	a, err := New(context.Background(), "/", client, &Options{Prefix: prefix, BlockTime: 20 * time.Millisecond, OperationTimeout: 200 * time.Millisecond, OnError: func(err error) {
		select {
		case failures <- err:
		default:
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := New(context.Background(), "/", direct, &Options{Prefix: prefix, BlockTime: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	srv := socketio.New(nil)
	ns := srv.DefaultNamespace()
	ns.SetAdapter(a)
	ns.EnableRecovery(nil)
	ns.OnConnect(func(s *socketio.Socket) { _ = s.Emit("seed") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: srv}
	go func() { _ = httpServer.Serve(ln) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	c := rawConnect(t, "http://"+ln.Addr().String(), "")
	_ = readSocketPacket(t, c)
	seed := readSocketPacket(t, c)
	if seed.Args()[0] != "seed" {
		t.Fatal(seed)
	}
	proxy.pause(true)
	select {
	case <-failures:
	case <-time.After(5 * time.Second):
		t.Fatal("stream reader did not detect Redis disconnect")
	}
	failed, _ := socketio.NewRecoveryRecord("/", nil, nil, "must-not-deliver", nil, false)
	if err = a.PublishRecord(failed); err == nil {
		t.Fatal("offline publish did not fail")
	}
	for _, name := range []string{"resume-one", "resume-two"} {
		record, _ := socketio.NewRecoveryRecord("/", nil, nil, name, nil, false)
		if err = b.PublishRecord(record); err != nil {
			t.Fatal(err)
		}
	}
	proxy.pause(false)
	for _, name := range []string{"resume-one", "resume-two"} {
		packet := readSocketPacket(t, c)
		if packet.Args()[0] != name {
			t.Fatalf("got %v, want %s", packet.Args(), name)
		}
	}
	barrier, _ := socketio.NewRecoveryRecord("/", nil, nil, "after-resume", nil, false)
	if err = b.PublishRecord(barrier); err != nil {
		t.Fatal(err)
	}
	if packet := readSocketPacket(t, c); packet.Args()[0] != "after-resume" {
		t.Fatalf("duplicate or failed append delivered: %v", packet.Args())
	}
	t.Log("PASS Redis disconnect resumes stream in order; failed append delivered nowhere")
}
