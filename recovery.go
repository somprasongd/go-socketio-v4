package socketio

import (
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"sync"
	"time"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// RecoveryOptions configure connection-state recovery: after an unexpected
// disconnection the namespace keeps the session — its id, rooms, data and
// the events sent to it — so a client returning within the window is
// restored instead of starting over. Wire-compatible with socket.io ≥ 4.6.
type RecoveryOptions struct {
	// MaxDisconnectionDuration is how long a disconnected session is kept.
	// Default 2 minutes, matching socket.io. Aimed at intermittent drops,
	// not indefinite persistence.
	MaxDisconnectionDuration time.Duration
	// SkipMiddlewares skips the connection middlewares when a recovery
	// succeeds. Default false, matching socket.io: middlewares run again,
	// so a blocked user cannot ride an old session past them.
	SkipMiddlewares bool
	// MaxSessions caps how many disconnected sessions are kept; the oldest
	// are dropped first. Default 10000.
	MaxSessions int
	// MaxBufferedEvents caps the replay buffer per session; the oldest
	// events are dropped first. Default 256.
	MaxBufferedEvents int
}

func (o *RecoveryOptions) withDefaults() *RecoveryOptions {
	d := *o
	if d.MaxDisconnectionDuration <= 0 {
		d.MaxDisconnectionDuration = 2 * time.Minute
	}
	if d.MaxSessions <= 0 {
		d.MaxSessions = 10000
	}
	if d.MaxBufferedEvents <= 0 {
		d.MaxBufferedEvents = 256
	}
	return &d
}

// recoveryEvent is one event sent to a recovery-enabled socket, kept in
// encoded wire form for cheap replay.
type recoveryEvent struct {
	offset int64
	text   string
	bins   [][]byte
}

// recoveryEntry is one recoverable session: its private id, the state to
// restore, and the event buffer. Lives for the whole session — connected
// or not — so the offset sequence never skips.
type recoveryEntry struct {
	mu sync.Mutex

	pid      string
	socketID string
	rooms    []string
	data     any

	offset    int64
	buffer    []recoveryEvent
	maxEvents int
	discAt    time.Time // zero while the session is connected
	attached  bool      // a live Socket is using this entry right now
}

// newRecoveryPID generates the private session id.
func newRecoveryPID() string {
	b := make([]byte, 15)
	if _, err := rand.Read(b); err != nil {
		panic("socketio: cannot read random bytes for recovery pid: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// recoveryStore holds the recoverable sessions of one namespace.
type recoveryStore struct {
	mu      sync.Mutex
	opts    *RecoveryOptions
	entries map[string]*recoveryEntry // by pid
}

func newRecoveryStore(opts *RecoveryOptions) *recoveryStore {
	if opts == nil {
		opts = &RecoveryOptions{}
	}
	return &recoveryStore{opts: opts.withDefaults(), entries: make(map[string]*recoveryEntry)}
}

// start creates a fresh entry for a new socket.
func (st *recoveryStore) start(socketID string) *recoveryEntry {
	st.purge()
	e := &recoveryEntry{pid: newRecoveryPID(), socketID: socketID, maxEvents: st.opts.MaxBufferedEvents}
	st.mu.Lock()
	st.entries[e.pid] = e
	st.mu.Unlock()
	return e
}

// lookup finds a session by pid, purging expired ones on the way.
func (st *recoveryStore) lookup(pid string) *recoveryEntry {
	st.purge()
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.entries[pid]
}

// drop forgets a session — used for deliberate disconnects, which socket.io
// does not recover.
func (st *recoveryStore) drop(pid string) {
	st.mu.Lock()
	delete(st.entries, pid)
	st.mu.Unlock()
}

// purge drops sessions past their window and enforces the session cap.
func (st *recoveryStore) purge() {
	now := time.Now()
	st.mu.Lock()
	defer st.mu.Unlock()
	for pid, e := range st.entries {
		e.mu.Lock()
		expired := !e.discAt.IsZero() && now.Sub(e.discAt) > st.opts.MaxDisconnectionDuration
		e.mu.Unlock()
		if expired {
			delete(st.entries, pid)
		}
	}
	for len(st.entries) > st.opts.MaxSessions {
		oldestPID := ""
		var oldest time.Time
		for pid, e := range st.entries {
			e.mu.Lock()
			at := e.discAt
			e.mu.Unlock()
			if oldestPID == "" || at.Before(oldest) {
				oldestPID, oldest = pid, at
			}
		}
		if oldestPID == "" {
			break
		}
		delete(st.entries, oldestPID)
	}
}

// trackRoom keeps the entry's room list live so a broadcast during the
// disconnection window can find the session.
func (e *recoveryEntry) trackRoom(room string, in bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	idx := -1
	for i, r := range e.rooms {
		if r == room {
			idx = i
			break
		}
	}
	if in && idx < 0 {
		e.rooms = append(e.rooms, room)
	}
	if !in && idx >= 0 {
		e.rooms = append(e.rooms[:idx], e.rooms[idx+1:]...)
	}
}

// broadcastToHeld buffers an event for every disconnected session whose
// rooms match — this is how events fired during the gap reach a client on
// replay. Held sessions cannot receive volatile events by definition.
func (st *recoveryStore) broadcastToHeld(rooms []string, event string, args []any, except map[string]struct{}, volatile bool) {
	if volatile {
		return
	}
	st.mu.Lock()
	held := make([]*recoveryEntry, 0, len(st.entries))
	for _, e := range st.entries {
		e.mu.Lock()
		isHeld := !e.discAt.IsZero() && !e.attached
		e.mu.Unlock()
		if isHeld {
			held = append(held, e)
		}
	}
	st.mu.Unlock()

	for _, e := range held {
		e.mu.Lock()
		if except != nil {
			if _, skip := except[e.socketID]; skip {
				e.mu.Unlock()
				continue
			}
		}
		matches := len(rooms) == 0
		for _, room := range rooms {
			for _, r := range e.rooms {
				if r == room {
					matches = true
					break
				}
			}
		}
		if !matches {
			e.mu.Unlock()
			continue
		}
		e.offset++
		off := e.offset
		full := make([]any, 0, len(args)+2)
		full = append(full, event)
		full = append(full, args...)
		full = append(full, strconv.FormatInt(off, 10))
		text, bins, err := parser.Encode(parser.Packet{Type: parser.Event, Data: full})
		if err != nil {
			e.mu.Unlock()
			continue
		}
		e.buffer = append(e.buffer, recoveryEvent{offset: off, text: text, bins: bins})
		if over := len(e.buffer) - e.maxEvents; over > 0 {
			e.buffer = e.buffer[over:]
		}
		e.mu.Unlock()
	}
}

// detach marks the session disconnected; recoverable tells whether the
// session may come back (deliberate disconnects drop it instead).
func (e *recoveryEntry) detach(recoverable bool) {
	e.mu.Lock()
	if recoverable {
		e.discAt = time.Now()
		e.attached = false
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
}

// stamp assigns the next offset to an outgoing event.
func (e *recoveryEntry) stamp() int64 {
	e.mu.Lock()
	e.offset++
	e.mu.Unlock()
	return e.offset
}

// remember files the encoded wire form into the replay buffer.
func (e *recoveryEntry) remember(max int, ev recoveryEvent) {
	e.mu.Lock()
	e.buffer = append(e.buffer, ev)
	if over := len(e.buffer) - max; over > 0 {
		e.buffer = e.buffer[over:]
	}
	e.mu.Unlock()
}

// replayAfter returns the buffered events the client has not seen, given
// the offset string it reported ("" means it has seen none of them).
func (e *recoveryEntry) replayAfter(clientOffset string) []recoveryEvent {
	var last int64
	if clientOffset != "" {
		if parsed, err := strconv.ParseInt(clientOffset, 10, 64); err == nil {
			last = parsed
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]recoveryEvent, 0, len(e.buffer))
	for _, ev := range e.buffer {
		if ev.offset > last {
			out = append(out, ev)
		}
	}
	return out
}
