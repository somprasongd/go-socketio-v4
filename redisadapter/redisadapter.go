// Package redisadapter relays socket.io broadcasts across processes through
// Redis pub/sub, so several server instances behind a load balancer can each
// hold part of the clients and still reach a room's members everywhere.
//
// Scope, matching how such adapters fundamentally work: room membership is
// tracked locally per process (each instance knows only its own sockets),
// and Broadcast publishes to the channel — every instance, this one
// included, delivers on subscription to its local members of the room.
// Membership queries (Members, SocketRooms, All) are therefore local-only.
package redisadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	socketio "github.com/somprasongd/go-socketio-v4"
)

// Adapter is a socketio.Adapter backed by Redis pub/sub.
type Adapter struct {
	local *socketio.InMemoryAdapter
	nsp   string
	ch    string
	rdb   redis.UniversalClient
	sub   *redis.PubSub

	mu        sync.Mutex
	closed    bool
	namespace *socketio.Namespace
	readyErr  error
}

// relayMessage is the wire format on the pub/sub channel.
type relayMessage struct {
	NSP      string            `json:"nsp"`
	Rooms    []string          `json:"rooms,omitempty"`
	Except   []string          `json:"except,omitempty"`
	Volatile bool              `json:"volatile,omitempty"`
	Event    string            `json:"event"`
	Args     []json.RawMessage `json:"args,omitempty"`
}

// New creates an adapter for one namespace. The subscription starts
// immediately; stop it with Close. channel must be shared by every process
// serving the same deployment.
func New(nsp string, rdb redis.UniversalClient, channel string) *Adapter {
	a := &Adapter{
		local: socketio.NewInMemoryAdapter(),
		nsp:   nsp,
		ch:    channel,
		rdb:   rdb,
	}
	a.sub = rdb.Subscribe(context.Background(), channel)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, a.readyErr = a.sub.Receive(ctx)
	cancel()
	go a.loop()
	return a
}

func (a *Adapter) loop() {
	for msg := range a.sub.Channel() {
		var relay relayMessage
		if err := json.Unmarshal([]byte(msg.Payload), &relay); err != nil {
			log.Printf("redisadapter: bad relay message: %v", err)
			continue
		}
		if relay.NSP != a.nsp {
			continue
		}
		args, err := decodeArgs(relay.Args)
		if err != nil {
			log.Printf("redisadapter: bad relay args: %v", err)
			continue
		}
		except := make(map[string]struct{}, len(relay.Except))
		for _, id := range relay.Except {
			except[id] = struct{}{}
		}
		a.mu.Lock()
		ns := a.namespace
		a.mu.Unlock()
		if ns != nil {
			ns.DeliverBroadcast(relay.Rooms, relay.Event, args, except, relay.Volatile)
		} else {
			socketio.DeliverLocal(a.local.Members(relay.Rooms), except, relay.Volatile, relay.Event, args)
		}
	}
}

// Close stops the subscription. The Redis connection itself belongs to the
// client the caller provided.
func (a *Adapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	a.mu.Unlock()
	return a.sub.Close()
}

func (a *Adapter) AddSocket(s *socketio.Socket)        { a.local.AddSocket(s) }
func (a *Adapter) RemoveSocket(s *socketio.Socket)     { a.local.RemoveSocket(s) }
func (a *Adapter) Add(s *socketio.Socket, room string) { a.local.Add(s, room) }
func (a *Adapter) Del(s *socketio.Socket, room string) { a.local.Del(s, room) }
func (a *Adapter) All() []*socketio.Socket             { return a.local.All() }
func (a *Adapter) Members(rooms []string) []*socketio.Socket {
	return a.local.Members(rooms)
}
func (a *Adapter) SocketRooms(s *socketio.Socket) []string { return a.local.SocketRooms(s) }

// Broadcast publishes the event on the channel; every process holding
// matching sockets delivers locally — including this one, via the
// subscription.
func (a *Adapter) Broadcast(rooms []string, event string, args []any, except map[string]struct{}, volatile bool) {
	relay := relayMessage{
		NSP:      a.nsp,
		Rooms:    rooms,
		Volatile: volatile,
		Event:    event,
	}
	for id := range except {
		relay.Except = append(relay.Except, id)
	}
	encoded, err := encodeArgs(args)
	if err != nil {
		log.Printf("redisadapter: cannot encode args: %v", err)
		return
	}
	relay.Args = encoded
	payload, err := json.Marshal(relay)
	if err != nil {
		log.Printf("redisadapter: cannot marshal relay: %v", err)
		return
	}
	if err := a.rdb.Publish(context.Background(), a.ch, payload).Err(); err != nil {
		log.Printf("redisadapter: publish failed: %v", err)
	}
}

// binaryMarker is how a []byte travels inside the JSON args: detached from
// the structure, base64-encoded, and spliced back on delivery.
type binaryMarker struct {
	Binary string `json:"$go-socketio-b64"`
}

func encodeArgs(args []any) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, len(args))
	for i, arg := range args {
		transformed := liftBinaryForJSON(arg)
		raw, err := json.Marshal(transformed)
		if err != nil {
			return nil, err
		}
		out[i] = raw
	}
	return out, nil
}

func liftBinaryForJSON(node any) any {
	switch v := node.(type) {
	case []byte:
		return binaryMarker{Binary: base64.StdEncoding.EncodeToString(v)}
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = liftBinaryForJSON(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = liftBinaryForJSON(item)
		}
		return out
	}
	return node
}

func decodeArgs(raw []json.RawMessage) ([]any, error) {
	out := make([]any, len(raw))
	for i, r := range raw {
		var node any
		if err := json.Unmarshal(r, &node); err != nil {
			return nil, err
		}
		out[i] = lowerBinaryFromJSON(node)
	}
	return out, nil
}

func lowerBinaryFromJSON(node any) any {
	switch v := node.(type) {
	case map[string]any:
		if len(v) == 1 {
			if b64, ok := v["$go-socketio-b64"].(string); ok {
				if data, err := base64.StdEncoding.DecodeString(b64); err == nil {
					return data
				}
			}
		}
		for k, item := range v {
			v[k] = lowerBinaryFromJSON(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = lowerBinaryFromJSON(item)
		}
		return v
	}
	return node
}

// BindNamespace routes incoming events through local recovery as well as live delivery.
func (a *Adapter) BindNamespace(ns *socketio.Namespace) { a.mu.Lock(); a.namespace = ns; a.mu.Unlock() }

// Ready reports whether the initial Redis subscription was established.
func (a *Adapter) Ready() error { return a.readyErr }
