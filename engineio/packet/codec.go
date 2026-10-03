package packet

import (
	"bytes"
	"encoding/base64"
	"fmt"
)

// Separator joins packets inside one polling payload. The v4 spec pins it to
// the ASCII record separator; the JS client is compiled against that byte, so
// nothing else can take its place.
const Separator byte = 0x1e

// Handshake is the JSON body of the Open packet. Field order here is the
// order engine.io emits, which keeps golden tests byte-comparable.
type Handshake struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"` // ms
	PingTimeout  int      `json:"pingTimeout"`  // ms
	MaxPayload   int      `json:"maxPayload"`
}

// EncodePacket renders one packet in its text form — "2probe", "0{...}",
// "bAQID" for binary — which is what a polling payload piece and a WebSocket
// text frame both carry.
func EncodePacket(p Packet) string { return p.String() }

// DecodePacket parses one text-form packet. Binary Message packets only ever
// arrive as "b"+base64 through polling or as a raw binary WebSocket frame
// (frame types carry the distinction there), so the text form never holds
// them without the prefix.
func DecodePacket(s string) (Packet, error) {
	return decodePiece([]byte(s))
}

// EncodePayload renders packets into one polling body. Text packets are
// joined by the record separator; binary packets travel as "b"+base64
// pieces, which is how the spec squeezes binary through text-only polling.
func EncodePayload(pkts []Packet) []byte {
	if len(pkts) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	for i, p := range pkts {
		if i > 0 {
			buf.WriteByte(Separator)
		}
		buf.WriteString(p.String())
	}
	return buf.Bytes()
}

// DecodePayload splits a polling body back into packets. An empty body is
// zero packets, not an error — an idle long-poll POST is legal.
func DecodePayload(data []byte) ([]Packet, error) {
	if len(data) == 0 {
		return nil, nil
	}
	pieces := bytes.Split(data, []byte{Separator})
	pkts := make([]Packet, 0, len(pieces))
	for i, piece := range pieces {
		if len(piece) == 0 {
			// A separator with nothing on one side cannot come from the
			// JS client; accepting it would hide framing bugs.
			return nil, fmt.Errorf("engineio/packet: empty packet piece %d in payload", i)
		}
		p, err := decodePiece(piece)
		if err != nil {
			return nil, err
		}
		pkts = append(pkts, p)
	}
	return pkts, nil
}

func decodePiece(piece []byte) (Packet, error) {
	if len(piece) == 0 {
		return Packet{}, fmt.Errorf("engineio/packet: empty packet")
	}
	switch piece[0] {
	case 'b':
		raw, err := base64.StdEncoding.DecodeString(string(piece[1:]))
		if err != nil {
			return Packet{}, fmt.Errorf("engineio/packet: bad base64 in binary piece: %w", err)
		}
		return Packet{Type: Message, Data: raw, Binary: true}, nil
	case '0', '1', '2', '3', '4', '5', '6':
		t := Type(piece[0] - '0')
		return Packet{Type: t, Data: piece[1:]}, nil
	}
	return Packet{}, fmt.Errorf("engineio/packet: %q is not a packet type", piece[0])
}
