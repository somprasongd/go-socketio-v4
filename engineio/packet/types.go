// Package packet implements the Engine.IO v4 wire format: the packet types
// and the payload encoding the polling transport carries. WebSocket framing
// builds on the same packet form (one frame per packet), so every transport
// shares this codec.
//
// Spec: https://socket.io/docs/v4/engine-io-protocol/
package packet

import (
	"encoding/base64"
	"fmt"
)

// Type is an Engine.IO packet type; the constants double as the leading
// character on the wire, which the v4 spec pins to ASCII '0'..'6'.
type Type byte

const (
	// Open is sent by the server on a new transport and carries the
	// handshake JSON (see Handshake).
	Open Type = 0
	// Close is sent by either side to end the session.
	Close Type = 1
	// Ping is sent by the server as the v4 heartbeat (or by a probing
	// client with payload "probe" before a transport upgrade).
	Ping Type = 2
	// Pong is the client's answer to a server heartbeat Ping.
	Pong Type = 3
	// Message carries application data: a text string or raw binary.
	Message Type = 4
	// Upgrade is sent by the client to accept the probed transport.
	Upgrade Type = 5
	// Noop flushes the old transport while an upgrade is in flight.
	Noop Type = 6
)

func (t Type) String() string {
	switch t {
	case Open:
		return "open"
	case Close:
		return "close"
	case Ping:
		return "ping"
	case Pong:
		return "pong"
	case Message:
		return "message"
	case Upgrade:
		return "upgrade"
	case Noop:
		return "noop"
	}
	return fmt.Sprintf("unknown(%d)", byte(t))
}

// Packet is one Engine.IO packet: a type plus its raw body. Text bodies are
// UTF-8 bytes (the Open handshake is JSON); a Message packet may instead
// carry opaque binary.
type Packet struct {
	Type Type
	Data []byte
	// Binary marks a Message packet whose Data is raw binary rather than
	// text. On the wire the distinction travels as the transport frame
	// (polling: "b"+base64, WebSocket: a binary frame), not as a byte.
	Binary bool
}

// String renders the packet the way it appears on the wire, which is what
// test failures and logs should show.
func (p Packet) String() string {
	if p.Binary {
		return "b" + base64.StdEncoding.EncodeToString(p.Data)
	}
	return string(rune('0'+byte(p.Type))) + string(p.Data)
}
