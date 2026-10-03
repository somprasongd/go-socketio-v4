package parser

import (
	"bytes"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
)

type customBinary struct{ Blob []byte }

func (c customBinary) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

type pointerBinary struct{ Blob []byte }

func (c *pointerBinary) MarshalJSON() ([]byte, error) { return []byte(`"pointer-custom"`), nil }

type embeddedBlob struct {
	Blob []byte `json:"blob"`
}
type namedBytes []byte

func TestTypedContainersCarryBinaryAttachments(t *testing.T) {
	tests := []struct {
		name  string
		value any
		bins  int
	}{
		{"map", map[string][]byte{"data": {1, 2, 3}}, 1},
		{"integer-map", map[int][]byte{7: {1, 2, 3}}, 1},
		{"slice", [][]byte{{1}, {2}}, 2},
		{"array", [2][]byte{{1}, {2}}, 2},
		{"named-bytes", namedBytes{1, 2, 3}, 1},
		{"struct", struct {
			Blob   []byte `json:"blob"`
			Hidden []byte `json:"-"`
			Empty  []byte `json:"empty,omitempty"`
		}{Blob: []byte{1}, Hidden: []byte{2}}, 1},
		{"embedded", struct{ embeddedBlob }{embeddedBlob{Blob: []byte{1}}}, 1},
		{"pointer", &embeddedBlob{Blob: []byte{1}}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			text, bins, err := Encode(Packet{Type: Event, Data: []any{"blob", tc.value}})
			if err != nil {
				t.Fatal(err)
			}
			if len(bins) != tc.bins {
				t.Fatalf("%s bins=%d want=%d", text, len(bins), tc.bins)
			}
			if _, err = Decode(text, bins); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBinaryStructPreservesJSONSemantics(t *testing.T) {
	payload := &struct {
		embeddedBlob
		Number  int64         `json:"number,string"`
		Exact   int64         `json:"exact"`
		Custom  customBinary  `json:"custom"`
		Pointer pointerBinary `json:"pointer"`
		IP      net.IP        `json:"ip"`
		Empty   []byte        `json:"empty,omitempty"`
	}{embeddedBlob: embeddedBlob{Blob: []byte{1}}, Number: 9007199254740993, Exact: 9007199254740993, IP: net.IPv4(127, 0, 0, 1)}
	text, bins, err := Encode(Packet{Type: Event, Data: []any{"payload", payload}})
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 {
		t.Fatal(len(bins), text)
	}
	for _, part := range []string{`"number":"9007199254740993"`, `"exact":9007199254740993`, `"custom":"custom"`, `"pointer":"pointer-custom"`, `"ip":"127.0.0.1"`} {
		if !strings.Contains(text, part) {
			t.Fatal(text, "missing", part)
		}
	}
	if strings.Contains(text, `"empty"`) {
		t.Fatal(text)
	}
}

func TestEmbeddedBinaryFieldDominanceAndNilPointers(t *testing.T) {
	type left struct{ Value []byte }
	type right struct{ Value []byte }
	type tagged struct {
		Other []byte `json:"Value"`
	}
	for _, payload := range []any{
		struct {
			left
			right
		}{left{[]byte{1}}, right{[]byte{2}}},
		struct {
			*embeddedBlob
			Label string
		}{Label: "nil"},
	} {
		original, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		text, bins, err := Encode(Packet{Type: Event, Data: []any{"event", payload}})
		if err != nil {
			t.Fatal(err)
		}
		if len(bins) != 0 || text != `2["event",`+string(original)+`]` {
			t.Fatal(text, bins)
		}
	}
	payload := struct {
		left
		tagged
	}{left{[]byte{1}}, tagged{[]byte{2}}}
	text, bins, err := Encode(Packet{Type: Event, Data: []any{"event", payload}})
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 || !bytes.Equal(bins[0], []byte{2}) {
		t.Fatal(text, bins)
	}
}

func TestBinaryLimitsAndCyclesReturnErrors(t *testing.T) {
	many := make([][]byte, MaxAttachments+1)
	if _, _, err := Encode(Packet{Type: Event, Data: []any{"many", many}}); err == nil {
		t.Fatal("outbound attachment cap not enforced")
	}
	if _, err := Decode(`511-["many"]`, make([][]byte, 11)); err == nil {
		t.Fatal("inbound attachment cap not enforced")
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	if _, _, err := Encode(Packet{Type: Event, Data: []any{"cycle", cyclic}}); err == nil {
		t.Fatal("cycle accepted")
	}
	// Non-binary typed values must retain normal encoding/json output.
	value := map[int][]int{1: {2, 3}}
	text, bins, err := Encode(Packet{Type: Event, Data: []any{"plain", value}})
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 0 || text != `2["plain",{"1":[2,3]}]` {
		t.Fatal(text, bins)
	}
	got, err := Decode(text, nil)
	if err != nil || !reflect.DeepEqual(got.Args()[1], map[string]any{"1": []any{float64(2), float64(3)}}) {
		t.Fatal(got, err)
	}
}
