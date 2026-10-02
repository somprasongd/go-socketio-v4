package engineio

import "net/http"

// serveWebSocket is filled in by the WebSocket transport milestone; until
// then sessions stay on polling and ws probes are refused cleanly.
func (s *Session) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "websocket transport not implemented", http.StatusNotImplemented)
}
