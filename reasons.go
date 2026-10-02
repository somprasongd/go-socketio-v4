package socketio

import (
	"github.com/somprasongd/go-socketio-v4/engineio"
)

// Disconnect reasons, in socket.io's own vocabulary. They reach
// Namespace.OnDisconnect handlers.
const (
	reasonClientDisconnect = "io client disconnect"
	reasonServerDisconnect = "io server disconnect"
	reasonTransportClose   = "transport close"
	reasonPingTimeout      = "ping timeout"
	reasonParseError       = "parse error"
	reasonServerShutdown   = "server shutting down"
)

// mapCloseReason translates an engine.io close reason into socket.io's
// disconnect vocabulary.
func mapCloseReason(r engineio.CloseReason) string {
	switch r {
	case engineio.CloseTransport:
		return reasonTransportClose
	case engineio.CloseTimeout:
		return reasonPingTimeout
	case engineio.CloseProtocol:
		return reasonParseError
	case engineio.CloseServer:
		return reasonServerShutdown
	}
	return string(r)
}
