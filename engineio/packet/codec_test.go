package packet

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestEncodePacketGolden(t *testing.T) {
	tests := []struct {
		name string
		pkt  Packet
		want string
	}{
		{"ping probe", Packet{Type: Ping, Data: []byte("probe")}, "2probe"},
		{"pong probe", Packet{Type: Pong, Data: []byte("probe")}, "3probe"},
		{"bare ping", Packet{Type: Ping}, "2"},
		{"close", Packet{Type: Close}, "1"},
		{"noop", Packet{Type: Noop}, "6"},
		{"upgrade", Packet{Type: Upgrade}, "5"},
		{"message text", Packet{Type: Message, Data: []byte("hello")}, "4hello"},
		{"message json", Packet{Type: Message, Data: []byte(`["hello",1]`)}, `4["hello",1]`},
		{"message binary", Packet{Type: Message, Data: []byte{0x01, 0x02, 0x03}, Binary: true}, "bAQID"},
		{"message empty binary", Packet{Type: Message, Data: []byte{}, Binary: true}, "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EncodePacket(tt.pkt); got != tt.want {
				t.Errorf("EncodePacket(%v) = %q, want %q", tt.pkt, got, tt.want)
			}
		})
	}
}

func TestDecodePacketGolden(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Packet
	}{
		{"ping probe", "2probe", Packet{Type: Ping, Data: []byte("probe")}},
		{"pong probe", "3probe", Packet{Type: Pong, Data: []byte("probe")}},
		{"bare close", "1", Packet{Type: Close}},
		{"message text", "4hello", Packet{Type: Message, Data: []byte("hello")}},
		{"message binary", "bAQID", Packet{Type: Message, Data: []byte{0x01, 0x02, 0x03}, Binary: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodePacket(tt.in)
			if err != nil {
				t.Fatalf("DecodePacket(%q) error: %v", tt.in, err)
			}
			if got.Type != tt.want.Type || !bytes.Equal(got.Data, tt.want.Data) || got.Binary != tt.want.Binary {
				t.Errorf("DecodePacket(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestDecodePacketErrors(t *testing.T) {
	for _, in := range []string{"", "9", "7", "x", "b!!!"} {
		if _, err := DecodePacket(in); err == nil {
			t.Errorf("DecodePacket(%q): expected error, got none", in)
		}
	}
}

func TestEncodePayloadGolden(t *testing.T) {
	tests := []struct {
		name string
		pkts []Packet
		want string
	}{
		{
			"upgrade probe exchange",
			[]Packet{
				{Type: Ping, Data: []byte("probe")},
				{Type: Pong, Data: []byte("probe")},
			},
			"2probe\x1e3probe",
		},
		{
			"text only",
			[]Packet{
				{Type: Message, Data: []byte("hi")},
				{Type: Noop},
			},
			"4hi\x1e6",
		},
		{
			"mixed text and binary",
			[]Packet{
				{Type: Message, Data: []byte("hi")},
				{Type: Message, Data: []byte{0x01, 0x02, 0x03}, Binary: true},
			},
			"4hi\x1ebAQID",
		},
		{"single packet", []Packet{{Type: Message, Data: []byte("solo")}}, "4solo"},
		{"empty", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodePayload(tt.pkts)
			if string(got) != tt.want {
				t.Errorf("EncodePayload = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecodePayloadRoundTrip(t *testing.T) {
	pkts := []Packet{
		{Type: Ping, Data: []byte("probe")},
		{Type: Message, Data: []byte(`["hello",1]`)},
		{Type: Message, Data: bytes.Repeat([]byte{0xff, 0x00, 0x7e, 0x1e}, 32), Binary: true},
	}
	encoded := EncodePayload(pkts)
	got, err := DecodePayload(encoded)
	if err != nil {
		t.Fatalf("DecodePayload error: %v", err)
	}
	if len(got) != len(pkts) {
		t.Fatalf("DecodePayload returned %d packets, want %d", len(got), len(pkts))
	}
	for i := range pkts {
		if got[i].Type != pkts[i].Type || got[i].Binary != pkts[i].Binary || !bytes.Equal(got[i].Data, pkts[i].Data) {
			t.Errorf("packet %d = %+v, want %+v", i, got[i], pkts[i])
		}
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"unknown type", "4hi\x1e9x"},
		{"bad base64", "b!!"},
		{"trailing separator", "2\x1e"},
		{"leading separator", "\x1e4hi"},
		{"bare separator", "\x1e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodePayload([]byte(tt.in)); err == nil {
				t.Errorf("DecodePayload(%q): expected error, got none", tt.in)
			}
		})
	}
}

func TestDecodePayloadEmpty(t *testing.T) {
	got, err := DecodePayload(nil)
	if err != nil || got != nil {
		t.Errorf("DecodePayload(nil) = %v, %v; want nil, nil", got, err)
	}
}

// The handshake body is byte-pinned so golden tests and any logging that
// echoes it stay stable across versions.
func TestHandshakeJSONGolden(t *testing.T) {
	h := Handshake{
		SID:          "lv_VI97",
		Upgrades:     []string{"websocket"},
		PingInterval: 25000,
		PingTimeout:  20000,
		MaxPayload:   1000000,
	}
	got, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("json.Marshal error: %v", err)
	}
	want := `{"sid":"lv_VI97","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`
	if string(got) != want {
		t.Errorf("handshake JSON =\n%s\nwant\n%s", got, want)
	}
}
