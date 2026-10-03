// Package redisstreamsadapter broadcasts over Redis Streams and restores
// disconnected Socket.IO sessions across server instances. Its Redis format
// is private to this Go library, not compatible with the JavaScript adapter.
package redisstreamsadapter

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	socketio "github.com/somprasongd/go-socketio-v4"
)

// DataCodec preserves application-defined socket data across processes.
// Implementations must be safe for concurrent calls.
type DataCodec interface {
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}
type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec) Decode(b []byte) (any, error) {
	var v any
	err := json.Unmarshal(b, &v)
	return v, err
}

// Options configure an isolated namespace stream. Set the same Prefix on all
// instances of a deployment. OnError must not block or call the namespace;
// callbacks can run in a delivery critical section. Errors omit event payloads.
type Options struct {
	Prefix           string
	MaxLen           int64
	ReadCount        int64
	BlockTime        time.Duration
	OperationTimeout time.Duration
	DataCodec        DataCodec
	OnError          func(error)
}

// Adapter retains room membership locally and packet history in Redis.
type Adapter struct {
	local                      *socketio.InMemoryAdapter
	rdb                        redis.UniversalClient
	nsp, base, stream, channel string
	opts                       Options
	ctx                        context.Context
	cancel                     context.CancelFunc
	sub                        *redis.PubSub
	wg                         sync.WaitGroup
	mu                         sync.RWMutex
	ns                         *socketio.Namespace
	closed                     bool
}

var _ socketio.Adapter = (*Adapter)(nil)
var _ socketio.RecoveryBackend = (*Adapter)(nil)
var _ socketio.RecordPublisher = (*Adapter)(nil)
var _ socketio.NamespaceBinder = (*Adapter)(nil)

// New establishes Redis connectivity and subscriptions before returning.
// The caller owns the Redis client. Close only closes adapter resources.
func New(ctx context.Context, namespace string, rdb redis.UniversalClient, options *Options) (*Adapter, error) {
	if rdb == nil {
		return nil, errors.New("redisstreamsadapter: nil Redis client")
	}
	if namespace == "" {
		namespace = "/"
	}
	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}
	o := Options{}
	if options != nil {
		o = *options
	}
	if o.Prefix == "" {
		o.Prefix = "go-socketio"
	}
	if o.MaxLen <= 0 {
		o.MaxLen = 100000
	}
	if o.ReadCount <= 0 {
		o.ReadCount = 100
	}
	if o.BlockTime <= 0 {
		o.BlockTime = time.Second
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = 5 * time.Second
	}
	if o.BlockTime >= o.OperationTimeout {
		return nil, errors.New("redisstreamsadapter: BlockTime must be less than OperationTimeout")
	}
	if o.DataCodec == nil {
		o.DataCodec = jsonCodec{}
	}
	if o.OnError == nil {
		o.OnError = func(err error) { log.Printf("redisstreamsadapter: %v", err) }
	}
	// Hash tags colocate stream, session and lease keys for atomic Redis scripts.
	sum := sha256.Sum256([]byte(o.Prefix + "\x00" + namespace))
	base := "sio:{" + hex.EncodeToString(sum[:16]) + "}"
	a := &Adapter{local: socketio.NewInMemoryAdapter(), rdb: rdb, nsp: namespace, base: base, stream: base + ":events", channel: base + ":ephemeral", opts: o}
	op, cancel := context.WithTimeout(ctx, o.OperationTimeout)
	defer cancel()
	if err := rdb.Ping(op).Err(); err != nil {
		return nil, errors.New("redisstreamsadapter: Redis connection failed")
	}
	cursor := "0-0"
	tail, err := rdb.XRevRangeN(op, a.stream, "+", "-", 1).Result()
	if err != nil {
		return nil, errors.New("redisstreamsadapter: stream initialization failed")
	}
	if len(tail) > 0 {
		cursor = tail[0].ID
	}
	a.sub = rdb.Subscribe(op, a.channel)
	if _, err := a.sub.Receive(op); err != nil {
		_ = a.sub.Close()
		return nil, errors.New("redisstreamsadapter: subscription failed")
	}
	a.ctx, a.cancel = context.WithCancel(ctx)
	a.wg.Add(2)
	go a.readLoop(cursor)
	go a.ephemeralLoop()
	return a, nil
}

func (a *Adapter) BindNamespace(ns *socketio.Namespace)      { a.mu.Lock(); a.ns = ns; a.mu.Unlock() }
func (a *Adapter) AddSocket(s *socketio.Socket)              { a.local.AddSocket(s) }
func (a *Adapter) RemoveSocket(s *socketio.Socket)           { a.local.RemoveSocket(s) }
func (a *Adapter) Add(s *socketio.Socket, room string)       { a.local.Add(s, room) }
func (a *Adapter) Del(s *socketio.Socket, room string)       { a.local.Del(s, room) }
func (a *Adapter) All() []*socketio.Socket                   { return a.local.All() }
func (a *Adapter) Members(rooms []string) []*socketio.Socket { return a.local.Members(rooms) }
func (a *Adapter) SocketRooms(s *socketio.Socket) []string   { return a.local.SocketRooms(s) }
func (a *Adapter) ReportError(err error)                     { a.opts.OnError(err) }

func (a *Adapter) Broadcast(rooms []string, event string, args []any, except map[string]struct{}, volatile bool) {
	r, err := socketio.NewRecoveryRecord(a.nsp, rooms, except, event, args, volatile)
	if err == nil {
		err = a.PublishRecord(r)
	}
	if err != nil {
		a.ReportError(err)
	}
}

func (a *Adapter) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.opts.OperationTimeout)
}

// PublishRecord durably appends before delivery, except for volatile packets.
func (a *Adapter) PublishRecord(record socketio.RecoveryRecord) error {
	a.mu.RLock()
	closed := a.closed
	a.mu.RUnlock()
	if closed || a.ctx.Err() != nil {
		return errors.New("redisstreamsadapter: adapter closed")
	}
	if record.Namespace != a.nsp {
		return errors.New("redisstreamsadapter: namespace mismatch")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return errors.New("redisstreamsadapter: packet serialization failed")
	}
	ctx, cancel := a.operation(a.ctx)
	defer cancel()
	if record.Volatile {
		if err := a.rdb.Publish(ctx, a.channel, data).Err(); err != nil {
			return errors.New("redisstreamsadapter: ephemeral publish failed")
		}
		return nil
	}
	if _, err = a.rdb.XAdd(ctx, &redis.XAddArgs{Stream: a.stream, MaxLen: a.opts.MaxLen, Values: map[string]any{"record": string(data)}}).Result(); err != nil {
		return errors.New("redisstreamsadapter: durable append failed; event not delivered")
	}
	return nil
}

func (a *Adapter) apply(record socketio.RecoveryRecord) {
	a.mu.RLock()
	ns := a.ns
	a.mu.RUnlock()
	if ns != nil {
		ns.DeliverRecord(record)
	}
}

func decodeRecord(msg redis.XMessage) (socketio.RecoveryRecord, error) {
	var r socketio.RecoveryRecord
	raw, ok := msg.Values["record"].(string)
	if !ok {
		return r, errors.New("redisstreamsadapter: invalid stream record")
	}
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return r, errors.New("redisstreamsadapter: invalid stream record")
	}
	r.Offset = msg.ID
	return r, nil
}

func (a *Adapter) readLoop(cursor string) {
	defer a.wg.Done()
	for a.ctx.Err() == nil {
		ctx, cancel := a.operation(a.ctx)
		streams, err := a.rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{a.stream, cursor}, Count: a.opts.ReadCount, Block: a.opts.BlockTime}).Result()
		cancel()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if a.ctx.Err() != nil {
				return
			}
			a.ReportError(errors.New("redisstreamsadapter: stream read failed; retrying"))
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-a.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				r, err := decodeRecord(msg)
				if err != nil {
					a.ReportError(err)
				} else {
					a.apply(r)
				}
				cursor = msg.ID
			}
		}
	}
}

func (a *Adapter) ephemeralLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.ctx.Done():
			return
		case msg, ok := <-a.sub.Channel():
			if !ok {
				return
			}
			var r socketio.RecoveryRecord
			if err := json.Unmarshal([]byte(msg.Payload), &r); err != nil {
				a.ReportError(errors.New("redisstreamsadapter: invalid ephemeral record"))
				continue
			}
			if !r.Volatile {
				continue
			}
			a.apply(r)
		}
	}
}

func (a *Adapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	a.mu.Unlock()
	a.cancel()
	err := a.sub.Close()
	a.wg.Wait()
	return err
}

type storedSession struct {
	socketio.RecoverySession
	Data []byte `json:"data"`
}

var saveScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
local now = redis.call('TIME')
redis.call('ZADD', KEYS[2], now[1] * 1000000 + now[2], KEYS[1])
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', (now[1] * 1000000 + now[2]) - ARGV[2]*1000)
for _, key in ipairs(expired) do redis.call('DEL', key); redis.call('ZREM', KEYS[2], key) end
local excess = redis.call('ZCARD', KEYS[2]) - tonumber(ARGV[3])
if excess > 0 then
 local old = redis.call('ZRANGE', KEYS[2], 0, excess-1)
 for _, key in ipairs(old) do redis.call('DEL', key); redis.call('ZREM', KEYS[2], key) end
end
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1`)

func (a *Adapter) sessionKey(pid string) string { return a.base + ":session:" + pid }
func (a *Adapter) claimKey(pid string) string   { return a.base + ":claim:" + pid }

func (a *Adapter) SaveSession(ctx context.Context, session socketio.RecoverySession, options socketio.RecoveryOptions) error {
	ctx, cancel := a.operation(ctx)
	defer cancel()
	data, err := a.opts.DataCodec.Encode(session.Data)
	if err != nil {
		return errors.New("redisstreamsadapter: session data encoding failed; session not saved")
	}
	raw, err := json.Marshal(storedSession{RecoverySession: session, Data: data})
	if err != nil {
		return errors.New("redisstreamsadapter: session serialization failed")
	}
	ttl := options.MaxDisconnectionDuration.Milliseconds()
	if ttl < 1 {
		ttl = 1
	}
	if _, err := saveScript.Run(ctx, a.rdb, []string{a.sessionKey(session.PID), a.base + ":sessions"}, raw, ttl, options.MaxSessions).Result(); err != nil {
		return errors.New("redisstreamsadapter: session persistence failed")
	}
	return nil
}

var claimScript = redis.NewScript(`
local session = redis.call('GET', KEYS[1])
if not session then return false end
if not redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', 10000) then return false end
return session`)
var finishScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
if ARGV[2] == '1' and redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if ARGV[2] == '1' then redis.call('DEL', KEYS[1]); redis.call('ZREM', KEYS[3], KEYS[1]) end
redis.call('DEL', KEYS[2])
return 1`)

func validOffset(id string) bool {
	p := strings.Split(id, "-")
	if len(p) != 2 {
		return false
	}
	for _, v := range p {
		if v == "" {
			return false
		}
		for _, c := range v {
			if c < '0' || c > '9' {
				return false
			}
		}
		if _, err := strconv.ParseUint(v, 10, 64); err != nil {
			return false
		}
	}
	return id != "0-0"
}

func (a *Adapter) ClaimSession(ctx context.Context, pid, offset string, options socketio.RecoveryOptions) (_ *socketio.RecoveryClaim, resultErr error) {
	if !validOffset(offset) {
		return nil, socketio.ErrRecoveryUnavailable
	}
	ctx, cancel := a.operation(ctx)
	defer cancel()
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, errors.New("redisstreamsadapter: claim token generation failed")
	}
	claim := &socketio.RecoveryClaim{Token: hex.EncodeToString(tokenBytes), Offset: offset, Session: socketio.RecoverySession{PID: pid}}
	raw, err := claimScript.Run(ctx, a.rdb, []string{a.sessionKey(pid), a.claimKey(pid)}, claim.Token).Text()
	if errors.Is(err, redis.Nil) {
		return nil, socketio.ErrRecoveryUnavailable
	}
	if err != nil {
		return nil, errors.New("redisstreamsadapter: session claim failed")
	}
	defer func() {
		if resultErr != nil {
			if err := a.FinishClaim(context.Background(), claim, false); err != nil {
				a.ReportError(err)
			}
		}
	}()
	var stored storedSession
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, errors.New("redisstreamsadapter: invalid saved session")
	}
	claim.Session = stored.RecoverySession
	issued := false
	for _, id := range claim.Session.IssuedOffsets {
		if id == offset {
			issued = true
			break
		}
	}
	if !issued {
		return nil, socketio.ErrRecoveryUnavailable
	}
	claim.Session.Data, err = a.opts.DataCodec.Decode(stored.Data)
	if err != nil {
		return nil, errors.New("redisstreamsadapter: session data decoding failed")
	}
	// An offset must still exist and must have targeted this socket. Clients
	// cannot claim an arbitrary global offset to skip missing events.
	seed, err := a.rdb.XRangeN(ctx, a.stream, offset, offset, 1).Result()
	if err != nil {
		return nil, errors.New("redisstreamsadapter: offset validation failed")
	}
	if len(seed) == 0 {
		return nil, socketio.ErrRecoveryUnavailable
	}
	first, err := decodeRecord(seed[0])
	if err != nil {
		return nil, err
	}
	if first.Namespace != a.nsp || first.Volatile {
		return nil, socketio.ErrRecoveryUnavailable
	}
	// Capture a finite fence; concurrent writers cannot extend this replay.
	tail, err := a.rdb.XRevRangeN(ctx, a.stream, "+", "-", 1).Result()
	if err != nil {
		return nil, errors.New("redisstreamsadapter: replay fence failed")
	}
	if len(tail) == 0 {
		return nil, socketio.ErrRecoveryUnavailable
	}
	claim.Fence = tail[0].ID
	cursor := offset
	for cursor != claim.Fence {
		entries, err := a.rdb.XRangeN(ctx, a.stream, "("+cursor, claim.Fence, a.opts.ReadCount).Result()
		if err != nil {
			return nil, errors.New("redisstreamsadapter: replay read failed")
		}
		if len(entries) == 0 {
			return nil, socketio.ErrRecoveryUnavailable
		}
		for _, msg := range entries {
			record, err := decodeRecord(msg)
			if err != nil {
				return nil, err
			}
			if matches(record, claim.Session, a.nsp) {
				claim.Records = append(claim.Records, record)
				if len(claim.Records) > options.MaxBufferedEvents {
					return nil, socketio.ErrRecoveryUnavailable
				}
			}
			cursor = msg.ID
		}
	}
	// Recheck after scanning to detect retention trimming during replay.
	if seed, err := a.rdb.XRangeN(ctx, a.stream, offset, offset, 1).Result(); err != nil {
		return nil, errors.New("redisstreamsadapter: replay coverage check failed")
	} else if len(seed) == 0 {
		return nil, socketio.ErrRecoveryUnavailable
	}
	return claim, nil
}

func matches(record socketio.RecoveryRecord, session socketio.RecoverySession, namespace string) bool {
	if record.Namespace != namespace || record.Volatile {
		return false
	}
	if _, excluded := record.Except[session.SID]; excluded {
		return false
	}
	if len(record.SocketIDs) > 0 {
		matched := false
		for _, id := range record.SocketIDs {
			if id == session.SID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(record.Rooms) == 0 {
		return true
	}
	for _, room := range record.Rooms {
		for _, joined := range session.Rooms {
			if room == joined {
				return true
			}
		}
	}
	return false
}

func (a *Adapter) FinishClaim(ctx context.Context, claim *socketio.RecoveryClaim, success bool) error {
	ctx, cancel := a.operation(ctx)
	defer cancel()
	flag := "0"
	if success {
		flag = "1"
	}
	n, err := finishScript.Run(ctx, a.rdb, []string{a.sessionKey(claim.Session.PID), a.claimKey(claim.Session.PID), a.base + ":sessions"}, claim.Token, flag).Int()
	if err != nil {
		return errors.New("redisstreamsadapter: claim completion failed")
	}
	if n != 1 {
		return errors.New("redisstreamsadapter: claim lease lost")
	}
	return nil
}

func (a *Adapter) InvalidateSession(ctx context.Context, pid string) error {
	ctx, cancel := a.operation(ctx)
	defer cancel()
	pipe := a.rdb.TxPipeline()
	pipe.Del(ctx, a.sessionKey(pid), a.claimKey(pid))
	pipe.ZRem(ctx, a.base+":sessions", a.sessionKey(pid))
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.New("redisstreamsadapter: session invalidation failed")
	}
	return nil
}

var renewScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], 10000)
return 1`)

// RefreshClaim extends replay to a new finite fence after application middleware.
func (a *Adapter) RefreshClaim(ctx context.Context, claim *socketio.RecoveryClaim, options socketio.RecoveryOptions) error {
	ctx, cancel := a.operation(ctx)
	defer cancel()
	n, err := renewScript.Run(ctx, a.rdb, []string{a.claimKey(claim.Session.PID), a.sessionKey(claim.Session.PID)}, claim.Token).Int()
	if err == nil && n == -1 {
		return socketio.ErrRecoveryUnavailable
	}
	if err != nil || n != 1 {
		return errors.New("redisstreamsadapter: claim lease lost")
	}
	tail, err := a.rdb.XRevRangeN(ctx, a.stream, "+", "-", 1).Result()
	if err != nil {
		return errors.New("redisstreamsadapter: replay refresh failed")
	}
	if len(tail) == 0 {
		return socketio.ErrRecoveryUnavailable
	}
	fence := tail[0].ID
	cursor := claim.Fence
	for cursor != fence {
		entries, err := a.rdb.XRangeN(ctx, a.stream, "("+cursor, fence, a.opts.ReadCount).Result()
		if err != nil {
			return errors.New("redisstreamsadapter: replay refresh read failed")
		}
		if len(entries) == 0 {
			return socketio.ErrRecoveryUnavailable
		}
		for _, msg := range entries {
			record, err := decodeRecord(msg)
			if err != nil {
				return err
			}
			if matches(record, claim.Session, a.nsp) {
				claim.Records = append(claim.Records, record)
				if len(claim.Records) > options.MaxBufferedEvents {
					return socketio.ErrRecoveryUnavailable
				}
			}
			cursor = msg.ID
		}
	}
	// The original offset must remain retained throughout middleware and refresh.
	if seed, err := a.rdb.XRangeN(ctx, a.stream, claim.Offset, claim.Offset, 1).Result(); err != nil {
		return errors.New("redisstreamsadapter: refresh coverage check failed")
	} else if len(seed) == 0 {
		return socketio.ErrRecoveryUnavailable
	}
	claim.Fence = fence
	return nil
}
