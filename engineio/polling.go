package engineio

import (
	"io"
	"net/http"
	"time"

	"github.com/somprasongd/go-socketio-v4/engineio/packet"
)

// pollCap bounds a parked long-poll GET. Heartbeat traffic guarantees the
// client needs a response within PingInterval, so the cap only exists to
// release dead connections and load balancers that time out idle requests.
// Tests shrink it.
var pollCap = 20 * time.Second

// writePollPayload answers a polling request with packets, text form joined
// by the record separator. Binary packets ride along as "b"+base64 pieces.
func writePollPayload(w http.ResponseWriter, pkts []packet.Packet) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(packet.EncodePayload(pkts))
}

// servePollingGet is the long-poll half of the polling transport: return
// buffered packets at once, or park until one arrives, the session ends, the
// client gives up, or the cap expires (flushed with a noop so the client's
// next poll starts fresh).
func (s *Session) servePollingGet(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	switch {
	case s.closed:
		s.mu.Unlock()
		writePollPayload(w, []packet.Packet{{Type: packet.Close}})
		return
	case s.ws != nil:
		// The session upgraded to WebSocket; a fresh poll would split the
		// stream in two, so this is a protocol violation, not a downgrade.
		s.closeLocked(CloseProtocol)
		s.mu.Unlock()
		http.Error(w, "session upgraded", http.StatusBadRequest)
		return
	case s.polling:
		s.mu.Unlock()
		http.Error(w, "overlapping polling request", http.StatusBadRequest)
		return
	}
	s.polling = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.polling = false
		s.mu.Unlock()
	}()

	for {
		s.mu.Lock()
		pkts := s.takeBufLocked()
		s.mu.Unlock()
		if len(pkts) > 0 {
			writePollPayload(w, pkts)
			return
		}
		select {
		case <-s.wake:
			// loop: re-check the buffer
		case <-s.done:
			// ended while parked: flush the close packet before going away
			s.mu.Lock()
			pkts := s.takeBufLocked()
			s.mu.Unlock()
			if len(pkts) == 0 {
				pkts = []packet.Packet{{Type: packet.Close}}
			}
			writePollPayload(w, pkts)
			return
		case <-r.Context().Done():
			return
		case <-time.After(pollCap):
			writePollPayload(w, []packet.Packet{{Type: packet.Noop}})
			return
		}
	}
}

// servePollingPost decodes a polling payload and feeds its packets to the
// session. It never parks; the response body is always "ok" on success.
func (s *Session) servePollingPost(w http.ResponseWriter, r *http.Request) {
	// The request cap leaves slack above MaxPayload so an oversized packet
	// is rejected by the per-packet check (which closes the session) rather
	// than tripping the transport limit first.
	limit := int64(s.srv.opts.MaxPayload)*4 + 4096
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		s.mu.Lock()
		s.closeLocked(CloseProtocol)
		s.mu.Unlock()
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	pkts, err := packet.DecodePayload(raw)
	if err != nil {
		s.mu.Lock()
		s.closeLocked(CloseProtocol)
		s.mu.Unlock()
		http.Error(w, "payload decode error", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	for _, p := range pkts {
		if s.closed {
			break
		}
		if size := 1 + len(p.Data); size > s.srv.opts.MaxPayload {
			s.closeLocked(CloseProtocol)
			break
		}
		s.handleIncomingLocked(p)
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
