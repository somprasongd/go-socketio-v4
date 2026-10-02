package parser

import (
	"bytes"
	"testing"
)

// Golden decodes use the exact examples from the protocol spec:
// https://socket.io/docs/v4/socket-io-protocol/
func TestDecodeGolden(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Packet
	}{
		{"connect no auth", "0", Packet{Type: Connect}},
		{
			"connect with auth",
			`0{"token":"123"}`,
			Packet{Type: Connect, Data: map[string]any{"token": "123"}},
		},
		{
			"connect ack with sid",
			`0{"sid":"lv_VI97"}`,
			Packet{Type: Connect, Data: map[string]any{"sid": "lv_VI97"}},
		},
		{
			"connect to nsp",
			`0/admin,`,
			Packet{Type: Connect, Namespace: "/admin"},
		},
		{"disconnect", "1", Packet{Type: Disconnect}},
		{"disconnect from nsp", "1/admin,", Packet{Type: Disconnect, Namespace: "/admin"}},
		{
			"event",
			`2["hello",1]`,
			Packet{Type: Event, Data: []any{"hello", float64(1)}},
		},
		{
			"event to nsp",
			`2/admin,["hello",1]`,
			Packet{Type: Event, Namespace: "/admin", Data: []any{"hello", float64(1)}},
		},
		{
			"event with ack id",
			`21["hello",1]`,
			Packet{Type: Event, ID: 1, HasID: true, Data: []any{"hello", float64(1)}},
		},
		{
			"event with nsp and ack id",
			`2/admin,456["project:delete",123]`,
			Packet{Type: Event, Namespace: "/admin", ID: 456, HasID: true,
				Data: []any{"project:delete", float64(123)}},
		},
		{
			"ack",
			`3["lorem ipsum"]`,
			Packet{Type: Ack, ID: 0, Data: []any{"lorem ipsum"}},
		},
		{
			"ack with id to nsp",
			`3/admin,456["project:deleted"]`,
			Packet{Type: Ack, Namespace: "/admin", ID: 456, HasID: true,
				Data: []any{"project:deleted"}},
		},
		{"bare ack", "3", Packet{Type: Ack, Data: []any{}}},
		{
			"connect error",
			`4{"message":"Not allowed"}`,
			Packet{Type: ConnectError, Data: map[string]any{"message": "Not allowed"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(tt.in, nil)
			if err != nil {
				t.Fatalf("Decode(%q): %v", tt.in, err)
			}
			if got.Type != tt.want.Type || got.Namespace != tt.want.Namespace ||
				got.ID != tt.want.ID || got.HasID != tt.want.HasID {
				t.Fatalf("header = %+v, want %+v", got, tt.want)
			}
			assertDeepEqual(t, got.Data, tt.want.Data)
		})
	}
}

func TestDecodeBinaryGolden(t *testing.T) {
	blob := []byte{1, 2, 3, 4}

	t.Run("binary event per spec", func(t *testing.T) {
		// spec example: 51-["hello",{"_placeholder":true,"num":0}]
		got, err := Decode(`51-["hello",{"_placeholder":true,"num":0}]`, [][]byte{blob})
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.Type != BinaryEvent {
			t.Fatalf("type = %v", got.Type)
		}
		args := got.Data.([]any)
		if args[0] != "hello" {
			t.Errorf("arg0 = %v", args[0])
		}
		if !bytes.Equal(args[1].([]byte), blob) {
			t.Errorf("arg1 = %x", args[1])
		}
	})

	t.Run("binary ack with id and nesting", func(t *testing.T) {
		got, err := Decode(`61-5["resp",{"_placeholder":true,"num":0}]`, [][]byte{blob})
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.Type != BinaryAck || !got.HasID || got.ID != 5 {
			t.Fatalf("header = %+v", got)
		}
		args := got.Data.([]any)
		if !bytes.Equal(args[1].([]byte), blob) {
			t.Errorf("arg1 = %x", args[1])
		}
	})

	t.Run("multiple attachments in deep positions", func(t *testing.T) {
		got, err := Decode(
			`52-["data",{"_placeholder":true,"num":0},{"nested":{"list":[{"_placeholder":true,"num":1}]}}]`,
			[][]byte{[]byte("first"), []byte("second")},
		)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		args := got.Data.([]any)
		if !bytes.Equal(args[1].([]byte), []byte("first")) {
			t.Errorf("arg1 = %x", args[1])
		}
		nested := args[2].(map[string]any)["nested"].(map[string]any)["list"].([]any)
		if !bytes.Equal(nested[0].([]byte), []byte("second")) {
			t.Errorf("nested = %x", nested[0])
		}
	})
}

func TestEncodeGolden(t *testing.T) {
	tests := []struct {
		name     string
		pkt      Packet
		want     string
		wantBins [][]byte
	}{
		{"connect", Packet{Type: Connect}, "0", nil},
		{
			"connect ack",
			Packet{Type: Connect, Data: map[string]any{"sid": "abc"}},
			`0{"sid":"abc"}`,
			nil,
		},
		{"connect nsp", Packet{Type: Connect, Namespace: "/admin"}, "0/admin,", nil},
		{"disconnect", Packet{Type: Disconnect}, "1", nil},
		{"disconnect nsp", Packet{Type: Disconnect, Namespace: "/admin"}, "1/admin,", nil},
		{
			"event",
			Packet{Type: Event, Data: []any{"hello", float64(1)}},
			`2["hello",1]`,
			nil,
		},
		{
			"event nsp ack id",
			Packet{Type: Event, Namespace: "/admin", ID: 456, HasID: true,
				Data: []any{"project:delete", float64(123)}},
			`2/admin,456["project:delete",123]`,
			nil,
		},
		{
			"ack with id",
			Packet{Type: Ack, ID: 7, HasID: true, Data: []any{"ok", true}},
			`37["ok",true]`,
			nil,
		},
		{"bare ack", Packet{Type: Ack, ID: 3, HasID: true}, "33", nil},
		{
			"connect error",
			Packet{Type: ConnectError, Data: map[string]any{"message": "Unable to connect"}},
			`4{"message":"Unable to connect"}`,
			nil,
		},
		{
			"binary event lifts bytes",
			Packet{Type: Event, Data: []any{"hello", []byte{1, 2, 3, 4}}},
			`51-["hello",{"_placeholder":true,"num":0}]`,
			[][]byte{{1, 2, 3, 4}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, bins, err := Encode(tt.pkt)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Encode = %q, want %q", got, tt.want)
			}
			if len(bins) != len(tt.wantBins) {
				t.Fatalf("attachments = %d, want %d", len(bins), len(tt.wantBins))
			}
		})
	}
}

// The wire form must round-trip through the JS client's expectations:
// encode → decode returns the same packet.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	blob := []byte{0x00, 0xff, 0x7e, 0x1e}
	pkts := []Packet{
		{Type: Connect},
		{Type: Connect, Data: map[string]any{"sid": "s1"}},
		{Type: Connect, Namespace: "/admin"},
		{Type: Disconnect, Namespace: "/admin"},
		{Type: Event, Data: []any{"card", map[string]any{"name": "สมชาย", "age": float64(30)}}},
		{Type: Event, Namespace: "/admin", ID: 9, HasID: true, Data: []any{"nested"}},
		{Type: Ack, ID: 12, HasID: true, Data: []any{float64(1), "two"}},
		{Type: Event, Data: []any{"bin", blob, []any{"deep", blob}}},
		{Type: Ack, ID: 2, HasID: true, Data: []any{blob}},
	}
	for i, p := range pkts {
		text, bins, err := Encode(p)
		if err != nil {
			t.Fatalf("pkt %d Encode: %v", i, err)
		}
		got, err := Decode(text, bins)
		if err != nil {
			t.Fatalf("pkt %d Decode(%q): %v", i, text, err)
		}
		// Events carrying binary decode back as the Binary wire type; that
		// is the protocol working as specified, not a round-trip failure.
		if got.Type.base() != p.Type.base() || got.Namespace != p.Namespace ||
			got.ID != p.ID || got.HasID != p.HasID {
			t.Errorf("pkt %d header = %+v, want %+v", i, got, p)
			continue
		}
		assertDeepEqual(t, got.Data, p.Data)
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		att  [][]byte
	}{
		{"empty", "", nil},
		{"bad type", "9", nil},
		{"letter type", "abc", nil},
		{"event no payload", "2", nil},
		{"event name not string", `2[1,2]`, nil},
		{"event not array", `2"hello"`, nil},
		{"bad json", `2["hello"`, nil},
		{"nsp without comma", "2/admin[", nil},
		{"connect payload not object", `0[1]`, nil},
		{"connect error not object", `4[1]`, nil},
		{"disconnect with payload", `1{"a":1}`, nil},
		{"binary without count dash", `5["x",{"_placeholder":true,"num":0}]`, [][]byte{{1}}},
		{"binary count mismatch", `51-["x",{"_placeholder":true,"num":0}]`, [][]byte{{1}, {2}}},
		{"binary without attachments", `51-["x",{"_placeholder":true,"num":0}]`, nil},
		{"attachment out of range", `51-["x",{"_placeholder":true,"num":3}]`, [][]byte{{1}}},
		{"placeholder missing", `51-["x"]`, [][]byte{{1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode(tt.in, tt.att); err == nil {
				t.Errorf("Decode(%q, %v): expected error, got none", tt.in, tt.att)
			}
		})
	}
}

func TestEncodeRejectsBinaryTypeInput(t *testing.T) {
	if _, _, err := Encode(Packet{Type: BinaryEvent}); err == nil {
		t.Error("expected error encoding BinaryEvent directly")
	}
}

func assertDeepEqual(t *testing.T, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			t.Fatalf("data = %#v, want %#v", got, want)
		}
		for i := range w {
			assertDeepEqual(t, g[i], w[i])
		}
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(g) != len(w) {
			t.Fatalf("data = %#v, want %#v", got, want)
		}
		for k, v := range w {
			assertDeepEqual(t, g[k], v)
		}
	case []byte:
		g, ok := got.([]byte)
		if !ok || !bytes.Equal(g, w) {
			t.Fatalf("data = %#v, want %#v", got, want)
		}
	default:
		if got != want {
			t.Fatalf("data = %#v (%T), want %#v (%T)", got, got, want, want)
		}
	}
}
