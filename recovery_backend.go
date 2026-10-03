package socketio

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/somprasongd/go-socketio-v4/parser"
)

// ErrRecoveryUnavailable means the session or complete packet history is unavailable.
// It permits a fresh connection, unlike backend operational failures.
var ErrRecoveryUnavailable = errors.New("socketio: recovery unavailable")

// RecoveryRecord is an immutable encoded event and its routing metadata.
// Text and Bins contain a Socket.IO packet without a recovery offset.
type RecoveryRecord struct {
	Namespace string              `json:"namespace"`
	SocketIDs []string            `json:"sockets,omitempty"`
	Rooms     []string            `json:"rooms,omitempty"`
	Except    map[string]struct{} `json:"except,omitempty"`
	Text      string              `json:"text"`
	Bins      [][]byte            `json:"bins,omitempty"`
	Volatile  bool                `json:"volatile,omitempty"`
	Offset    string              `json:"-"`
}

// NewRecoveryRecord encodes an event once, preserving binary attachments.
func NewRecoveryRecord(namespace string, rooms []string, except map[string]struct{}, event string, args []any, volatile bool) (RecoveryRecord, error) {
	text, bins, err := parser.Encode(parser.Packet{Type: parser.Event, Namespace: nsName(namespace), Data: append([]any{event}, args...)})
	return RecoveryRecord{Namespace: namespace, Rooms: rooms, Except: except, Text: text, Bins: bins, Volatile: volatile}, err
}

// RecoverySession is a snapshot taken before removing a disconnected socket.
// Data must be supported by the backend's codec.
type RecoverySession struct {
	PID           string   `json:"pid"`
	SID           string   `json:"sid"`
	Rooms         []string `json:"rooms"`
	Data          any      `json:"-"`
	LastOffset    string   `json:"lastOffset"`
	IssuedOffsets []string `json:"issuedOffsets"`
}

// RecoveryClaim reserves a disconnected session until FinishClaim. Fence is
// the inclusive log high-water mark used to distinguish replay from live events.
type RecoveryClaim struct {
	Session RecoverySession
	Records []RecoveryRecord
	Token   string
	Fence   string
	Offset  string
}

// RecoveryBackend is optional; ordinary adapters keep using local recovery.
// Claim must validate complete history, atomically reserve the session, and
// return ErrRecoveryUnavailable for expiry, contention or missing history.
// FinishClaim consumes a successful reservation or releases a failed one.
type RecoveryBackend interface {
	SaveSession(context.Context, RecoverySession, RecoveryOptions) error
	ClaimSession(context.Context, string, string, RecoveryOptions) (*RecoveryClaim, error)
	RefreshClaim(context.Context, *RecoveryClaim, RecoveryOptions) error
	FinishClaim(context.Context, *RecoveryClaim, bool) error
	InvalidateSession(context.Context, string) error
}

// RecordPublisher owns durable publish and delivery. PublishRecord must not
// deliver a durable event when persistence fails. ReportError handles errors
// from legacy broadcast APIs that do not return an error.
type RecordPublisher interface {
	PublishRecord(RecoveryRecord) error
	ReportError(error)
}

func (ns *Namespace) recoveryBackend() RecoveryBackend {
	if ns.recoveryFields() == nil {
		return nil
	}
	b, _ := ns.adapterOf().(RecoveryBackend)
	return b
}

func (ns *Namespace) persistSession(s *Socket, backend RecoveryBackend) {
	s.eventMu.Lock()
	s.rec.mu.Lock()
	snapshot := RecoverySession{PID: s.rec.pid, SID: s.ID(), Rooms: append([]string(nil), s.rec.rooms...), Data: s.rec.data, LastOffset: s.rec.lastOffset, IssuedOffsets: append([]string(nil), s.rec.issuedOffsets...)}
	s.rec.mu.Unlock()
	s.eventMu.Unlock()
	if err := backend.SaveSession(context.Background(), snapshot, *ns.recoveryOpts()); err != nil {
		ns.reportBackendError(err)
	}
}

func (ns *Namespace) invalidateSession(pid string, backend RecoveryBackend) {
	if err := backend.InvalidateSession(context.Background(), pid); err != nil {
		ns.reportBackendError(err)
	}
}

func (ns *Namespace) reportBackendError(err error) {
	if p, ok := ns.adapterOf().(RecordPublisher); ok {
		p.ReportError(err)
	}
}

// DeliverRecord applies a stream record locally. The adapter must call it in
// stream order. Held sessions are recovered from the shared log, not duplicated
// into local buffers. Volatile records are never replayed.
func (ns *Namespace) DeliverRecord(record RecoveryRecord) {
	if record.Namespace != ns.name {
		return
	}
	ns.deliveryMu.Lock()
	defer ns.deliveryMu.Unlock()
	recipients := ns.adapterOf().Members(record.Rooms)
	if len(record.SocketIDs) > 0 {
		recipients = ns.adapterOf().All()
	}
	for _, s := range recipients {
		if len(record.SocketIDs) > 0 {
			matched := false
			for _, id := range record.SocketIDs {
				if id == s.ID() {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if _, excluded := record.Except[s.ID()]; excluded {
			continue
		}
		if err := s.deliverRecord(record); err != nil {
			ns.reportBackendError(err)
		}
	}
}

func recordWire(record RecoveryRecord, withOffset bool) (string, [][]byte, error) {
	if !withOffset || record.Offset == "" || record.Volatile {
		return record.Text, record.Bins, nil
	}
	packet, err := parser.Decode(record.Text, record.Bins)
	if err != nil {
		return "", nil, err
	}
	if packet.Type == parser.BinaryEvent {
		packet.Type = parser.Event
	}
	packet.Data = append(packet.Args(), record.Offset)
	return parser.Encode(packet)
}

// compareOffset compares Redis stream IDs numerically (lexical ordering is wrong).
func compareOffset(a, b string) int {
	parse := func(s string) (uint64, uint64) {
		parts := strings.SplitN(s, "-", 2)
		x, _ := strconv.ParseUint(parts[0], 10, 64)
		var y uint64
		if len(parts) == 2 {
			y, _ = strconv.ParseUint(parts[1], 10, 64)
		}
		return x, y
	}
	ax, ay := parse(a)
	bx, by := parse(b)
	if ax < bx || ax == bx && ay < by {
		return -1
	}
	if ax == bx && ay == by {
		return 0
	}
	return 1
}

// sendOrQueue runs with eventMu held. Restore keeps live sends behind replay
// and OnConnect without holding namespace locks while invoking user handlers.
func (s *Socket) sendOrQueue(record RecoveryRecord) error {
	if s.restoring {
		s.pending = append(s.pending, record)
		return nil
	}
	err := s.c.sendEncoded(record.Text, record.Bins)
	if err == nil {
		s.markIssued(record.Offset)
	}
	return err
}

func (s *Socket) deliverRecord(record RecoveryRecord) error {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if record.Volatile && !s.c.sess.Writable() {
		return nil
	}
	if s.rec != nil && record.Offset != "" {
		s.rec.mu.Lock()
		if compareOffset(record.Offset, s.rec.lastOffset) <= 0 {
			s.rec.mu.Unlock()
			return nil
		}
		s.rec.lastOffset = record.Offset
		s.rec.mu.Unlock()
	}
	text, bins, err := recordWire(record, s.rec != nil)
	if err != nil {
		return err
	}
	return s.sendOrQueue(RecoveryRecord{Text: text, Bins: bins, Offset: record.Offset})
}

func (s *Socket) finishRestore() {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	s.restoring = false
	for _, record := range s.pending {
		if err := s.c.sendEncoded(record.Text, record.Bins); err == nil {
			s.markIssued(record.Offset)
		}
	}
	s.pending = nil
}

// restoreExternal holds the namespace fence across claim, replay and socket
// registration. Incoming delivery waits, then deduplicates through Fence.
func (c *client) restoreExternal(ns *Namespace, backend RecoveryBackend, pid, offset string, auth map[string]any) bool {
	ns.deliveryMu.Lock()
	claim, err := backend.ClaimSession(context.Background(), pid, offset, *ns.recoveryOpts())
	if err != nil {
		ns.deliveryMu.Unlock()
		if errors.Is(err, ErrRecoveryUnavailable) {
			return false
		}
		ns.reportBackendError(err)
		_ = c.sendPacket(parser.Packet{Type: parser.ConnectError, Namespace: nsName(ns.name), Data: map[string]any{"message": "Recovery backend unavailable; retry", "retryable": true}})
		return true
	}
	success := false
	defer func() {
		if !success {
			if err := backend.FinishClaim(context.Background(), claim, false); err != nil {
				ns.reportBackendError(err)
			}
		}
	}()
	s := &Socket{id: claim.Session.SID, ns: ns, c: c, handshake: auth, data: claim.Session.Data, recovered: true, restoring: true, preparedRooms: preparedRooms(claim.Session.Rooms)}
	ns.deliveryMu.Unlock()
	s.rec = &recoveryEntry{pid: claim.Session.PID, socketID: s.id, external: true, attached: true, lastOffset: claim.Fence, maxEvents: ns.recoveryOpts().MaxBufferedEvents, issuedOffsets: append([]string(nil), claim.Session.IssuedOffsets...)}
	if !ns.recoveryOpts().SkipMiddlewares {
		if err := ns.runMiddlewares(s); err != nil {
			_ = c.sendPacket(parser.Packet{Type: parser.ConnectError, Namespace: nsName(ns.name), Data: map[string]any{"message": err.Error()}})
			return true
		}
	}
	ns.deliveryMu.Lock()
	if err := backend.RefreshClaim(context.Background(), claim, *ns.recoveryOpts()); err != nil {
		ns.deliveryMu.Unlock()
		if errors.Is(err, ErrRecoveryUnavailable) {
			return false
		}
		ns.reportBackendError(err)
		_ = c.sendPacket(parser.Packet{Type: parser.ConnectError, Namespace: nsName(ns.name), Data: map[string]any{"message": "Recovery reservation unavailable; retry", "retryable": true}})
		return true
	}
	s.rec.lastOffset = claim.Fence
	if err := backend.FinishClaim(context.Background(), claim, true); err != nil {
		ns.deliveryMu.Unlock()
		ns.reportBackendError(err)
		_ = c.sendPacket(parser.Packet{Type: parser.ConnectError, Namespace: nsName(ns.name), Data: map[string]any{"message": "Recovery reservation unavailable; retry", "retryable": true}})
		return true
	}
	success = true
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		ns.deliveryMu.Unlock()
		if err := backend.SaveSession(context.Background(), claim.Session, *ns.recoveryOpts()); err != nil {
			ns.reportBackendError(err)
		}
		return true
	}
	c.conns[ns.name] = s
	c.mu.Unlock()
	ns.addSocket(s)
	err = c.sendPacket(parser.Packet{Type: parser.Connect, Namespace: nsName(ns.name), Data: map[string]any{"sid": s.id, "pid": s.rec.pid}})
	if err == nil {
		for _, record := range claim.Records {
			text, bins, encodeErr := recordWire(record, true)
			if encodeErr != nil {
				err = encodeErr
				break
			}
			if err = c.sendEncoded(text, bins); err != nil {
				break
			}
			s.markIssued(record.Offset)
		}
	}
	if err != nil {
		ns.removeSocket(s)
		c.mu.Lock()
		delete(c.conns, ns.name)
		c.mu.Unlock()
		ns.deliveryMu.Unlock()
		ns.reportBackendError(err)
		if saveErr := backend.SaveSession(context.Background(), claim.Session, *ns.recoveryOpts()); saveErr != nil {
			ns.reportBackendError(saveErr)
		}
		c.sess.Close()
		return true
	}
	ns.deliveryMu.Unlock()
	ns.fireConnect(s)
	s.finishRestore()
	return true
}

// attach completes a reserved memory recovery entry.
func (e *recoveryEntry) attach() {
	e.mu.Lock()
	e.discAt = time.Time{}
	e.attached = true
	e.claimed = false
	e.mu.Unlock()
}

// markIssued records only offsets successfully written to the transport.
func (s *Socket) markIssued(offset string) {
	if s.rec == nil || !s.rec.external || offset == "" {
		return
	}
	s.rec.mu.Lock()
	defer s.rec.mu.Unlock()
	out := s.rec.issuedOffsets[:0]
	for _, previous := range s.rec.issuedOffsets {
		if previous != offset {
			out = append(out, previous)
		}
	}
	s.rec.issuedOffsets = append(out, offset)
	if n := len(s.rec.issuedOffsets) - s.rec.maxEvents; n > 0 {
		s.rec.issuedOffsets = s.rec.issuedOffsets[n:]
	}
}
