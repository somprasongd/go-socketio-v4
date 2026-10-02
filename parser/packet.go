// Package parser implements the Socket.IO v4 wire format — the packet layer
// that rides inside Engine.IO Message packets (see the engineio package).
// It encodes and decodes CONNECT/EVENT/ACK/CONNECT_ERROR packets, including
// the binary-attachment scheme of types 5 and 6.
//
// Spec: https://socket.io/docs/v4/socket-io-protocol/
package parser

import (
	"fmt"
)

// Type is a Socket.IO packet type; the constants double as the leading
// character on the wire, pinned by the spec to ASCII '0'..'6'.
type Type byte

const (
	// Connect opens a namespace. Client→server the payload is the auth
	// object (or empty); server→client it is {"sid":...}.
	Connect Type = 0
	// Disconnect leaves the namespace.
	Disconnect Type = 1
	// Event is an application event: [name, ...args].
	Event Type = 2
	// Ack answers an event that requested an acknowledgement: [...args].
	Ack Type = 3
	// ConnectError rejects a namespace connection: {"message":...}.
	ConnectError Type = 4
	// BinaryEvent is an Event whose args contain binary values, carried as
	// attachments after the text part.
	BinaryEvent Type = 5
	// BinaryAck is an Ack with binary args.
	BinaryAck Type = 6
)

func (t Type) String() string {
	switch t {
	case Connect:
		return "connect"
	case Disconnect:
		return "disconnect"
	case Event:
		return "event"
	case Ack:
		return "ack"
	case ConnectError:
		return "connect_error"
	case BinaryEvent:
		return "binary_event"
	case BinaryAck:
		return "binary_ack"
	}
	return fmt.Sprintf("unknown(%d)", byte(t))
}

// Packet is one decoded Socket.IO packet.
type Packet struct {
	Type Type
	// Namespace is the target namespace: "" for the default one, otherwise
	// "/admin" style (no trailing comma — that is wire syntax only).
	Namespace string
	// Attachments carries the binary payloads of BinaryEvent/BinaryAck
	// packets. On decode they have already been spliced into Data; the
	// field keeps them for callers that want the raw bytes.
	Attachments [][]byte
	// ID is the acknowledgement id; HasID says whether one was present.
	ID    int64
	HasID bool
	// Data holds the payload per type:
	//   Event/BinaryEvent/Ack/BinaryAck → []any (binary leaves are []byte)
	//   Connect/ConnectError            → map[string]any (may be nil)
	//   Disconnect                      → nil
	Data any
}

// attachmentPlaceholder is the JSON marker the JS client puts where a binary
// value belongs; attachments[i] replaces the marker with num == i.
type attachmentPlaceholder struct {
	Placeholder bool `json:"_placeholder"`
	Num         int  `json:"num"`
}

// eventType returns the wire type a packet takes given whether its payload
// contains binary values.
func (t Type) withBinary(binary bool) Type {
	if !binary {
		return t
	}
	switch t {
	case Event:
		return BinaryEvent
	case Ack:
		return BinaryAck
	}
	return t
}

func (t Type) base() Type {
	if t == BinaryEvent {
		return Event
	}
	if t == BinaryAck {
		return Ack
	}
	return t
}

// Args returns the packet's payload as an argument list, for the event
// family. Packets without a list yield nil.
func (p Packet) Args() []any {
	args, _ := p.Data.([]any)
	return args
}

// argsOf is the package-internal alias of Args.
func (p Packet) argsOf() []any { return p.Args() }

// errf is a small helper keeping decode errors uniform.
func errf(format string, a ...any) error {
	return fmt.Errorf("parser: "+format, a...)
}
