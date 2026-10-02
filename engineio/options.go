// Package engineio implements the Engine.IO v4 server: session management,
// the polling and WebSocket transports, and the polling-to-WebSocket upgrade.
// It is usable on its own (a plain http.Handler) and is the transport layer
// the socket.io package sits on.
//
// Spec: https://socket.io/docs/v4/engine-io-protocol/
package engineio

import "time"

// Defaults mirror engine.io's own, so a client negotiates the same timings
// it would against the reference server.
const (
	DefaultPingInterval = 25 * time.Second
	DefaultPingTimeout  = 20 * time.Second
	DefaultMaxPayload   = 1_000_000
)

// Options configure the server. Zero fields fall back to the defaults above.
type Options struct {
	// PingInterval is how often the client sends a Ping packet. Sent in the
	// handshake in milliseconds.
	PingInterval time.Duration
	// PingTimeout is how long after a ping the client may stay silent.
	// The server closes a session once PingInterval+PingTimeout pass with
	// nothing received.
	PingTimeout time.Duration
	// MaxPayload is the largest single packet accepted or sent, in bytes,
	// matching engine.io's per-packet limit.
	MaxPayload int
	// AllowedOrigins lists the Origin header values allowed to open a
	// connection. Empty, or a single "*", allows every origin — the
	// engine.io default, right for loopback-only deployments. Anything
	// else is refused with 403 on both polling and WebSocket.
	AllowedOrigins []string
}

// allowsOrigin reports whether the given Origin header value may connect.
func (o *Options) allowsOrigin(origin string) bool {
	if len(o.AllowedOrigins) == 0 {
		return true
	}
	for _, allowed := range o.AllowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

func (o *Options) withDefaults() *Options {
	d := *o
	if d.PingInterval <= 0 {
		d.PingInterval = DefaultPingInterval
	}
	if d.PingTimeout <= 0 {
		d.PingTimeout = DefaultPingTimeout
	}
	if d.MaxPayload <= 0 {
		d.MaxPayload = DefaultMaxPayload
	}
	return &d
}
