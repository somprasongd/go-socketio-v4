package parser

import (
	"encoding/json"
	"strconv"
	"strings"
)

// MaxAttachments bounds binary reconstruction memory per packet.
const MaxAttachments = 10

// NeededAttachments reports how many binary attachments a text packet
// expects, or -1 when the type carries none. Callers use it to know whether
// the binary Engine.IO packets that follow are part of this packet.
func NeededAttachments(text string) int {
	if len(text) == 0 || text[0] != '5' && text[0] != '6' {
		return -1
	}
	dash := strings.IndexByte(text[1:], '-')
	if dash < 0 {
		return 0 // Decode reports the malformed count
	}
	n, err := strconv.Atoi(text[1 : 1+dash])
	if err != nil {
		return 0
	}
	return n
}

// Decode parses one Socket.IO text packet and splices the attachments that
// followed it (binary Engine.IO packets) into the payload. attachments must
// be nil for the plain-text types.
func Decode(text string, attachments [][]byte) (Packet, error) {
	if text == "" {
		return Packet{}, errf("empty packet")
	}
	if text[0] < '0' || text[0] > '6' {
		return Packet{}, errf("%q is not a packet type", text[0])
	}
	t := Type(text[0] - '0')
	rest := text[1:]

	// Binary types lead with the attachment count and a dash. Zero declared
	// attachments with none given stays legal (`50-[]`).
	if t == BinaryEvent || t == BinaryAck {
		dash := strings.IndexByte(rest, '-')
		if dash < 0 {
			return Packet{}, errf("%s is missing the attachment count", t)
		}
		n, err := strconv.Atoi(rest[:dash])
		if err != nil || n < 0 || n > MaxAttachments || n != len(attachments) {
			return Packet{}, errf("attachment count %q does not match the %d attachments given", rest[:dash], len(attachments))
		}
		rest = rest[dash+1:]
	} else {
		attachments = nil
	}

	p := Packet{Type: t}

	// Namespace: "/...,", present only for non-default namespaces.
	if strings.HasPrefix(rest, "/") {
		comma := strings.IndexByte(rest, ',')
		if comma < 0 {
			return Packet{}, errf("namespace %q is missing its comma", rest)
		}
		p.Namespace = rest[:comma]
		rest = rest[comma+1:]
	}

	// Ack id: leading digits on the event family.
	if t == Event || t == Ack || t == BinaryEvent || t == BinaryAck {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i > 0 {
			id, err := strconv.ParseInt(rest[:i], 10, 64)
			if err != nil {
				return Packet{}, errf("bad ack id %q", rest[:i])
			}
			p.ID, p.HasID = id, true
			rest = rest[i:]
		}
	}

	// Payload.
	var data any
	if rest != "" {
		if err := json.Unmarshal([]byte(rest), &data); err != nil {
			return Packet{}, errf("bad JSON %q: %v", rest, err)
		}
	}
	switch t {
	case Event, BinaryEvent, Ack, BinaryAck:
		args, ok := data.([]any)
		if !ok {
			if data == nil && t.base() == Ack {
				args = []any{} // an ack may carry zero arguments
			} else {
				return Packet{}, errf("%s payload must be an array", t)
			}
		}
		if t.base() == Event {
			if len(args) == 0 {
				return Packet{}, errf("event payload needs an event name")
			}
			if _, ok := args[0].(string); !ok {
				return Packet{}, errf("event name must be a string")
			}
		}
		if t == BinaryEvent || t == BinaryAck {
			spliced, seen, err := spliceAttachments(args, attachments, 0)
			if err != nil {
				return Packet{}, err
			}
			if seen != len(attachments) {
				return Packet{}, errf("packet declares %d attachments but carries %d placeholders", len(attachments), seen)
			}
			data = spliced
		} else {
			data = args
		}
	case Connect, ConnectError:
		if data != nil {
			obj, ok := data.(map[string]any)
			if !ok {
				return Packet{}, errf("%s payload must be an object", t)
			}
			data = obj
		}
	case Disconnect:
		if rest != "" {
			return Packet{}, errf("disconnect takes no payload")
		}
	}
	p.Data = data
	return p, nil
}

// spliceAttachments replaces every {"_placeholder":true,"num":N} marker with
// attachments[N], counting them as it goes so the declared total is checked.
func spliceAttachments(node any, attachments [][]byte, seen int) (any, int, error) {
	switch v := node.(type) {
	case map[string]any:
		if ph, ok := placeholderOf(v); ok {
			if ph < 0 || ph >= len(attachments) {
				return nil, seen, errf("attachment %d out of range", ph)
			}
			return attachments[ph], seen + 1, nil
		}
		out := make(map[string]any, len(v))
		for k, val := range v {
			nv, n, err := spliceAttachments(val, attachments, seen)
			if err != nil {
				return nil, n, err
			}
			seen = n
			out[k] = nv
		}
		return out, seen, nil
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			nv, n, err := spliceAttachments(val, attachments, seen)
			if err != nil {
				return nil, n, err
			}
			seen = n
			out[i] = nv
		}
		return out, seen, nil
	}
	return node, seen, nil
}

// placeholderOf recognises the binary marker shape the JS client emits.
func placeholderOf(m map[string]any) (int, bool) {
	if len(m) != 2 {
		return 0, false
	}
	ph, ok := m["_placeholder"].(bool)
	if !ok || !ph {
		return 0, false
	}
	num, ok := m["num"].(float64)
	if !ok {
		return 0, false
	}
	return int(num), true
}

// Encode renders a packet into its text form plus the attachments to send
// after it as binary Engine.IO packets. Binary values inside the event/ack
// args ([]byte, at any depth) are lifted out into attachments automatically;
// pass Event/Ack even when the args contain binary.
func Encode(p Packet) (string, [][]byte, error) {
	base := p.Type.base()
	if p.Type != base {
		// Callers pass Event/Ack; the binary wire types are derived from
		// whether the args actually contain binary.
		return "", nil, errf("encode %s, not %s", base, p.Type)
	}
	if p.Namespace != "" && !strings.HasPrefix(p.Namespace, "/") {
		return "", nil, errf("namespace %q must start with /", p.Namespace)
	}

	var attachments [][]byte
	args := p.argsOf()
	if base == Event || base == Ack {
		trimmed := make([]any, 0, len(args))
		for _, a := range args {
			stripped, err := liftBinaries(a, &attachments, 0)
			if err != nil {
				return "", nil, err
			}
			trimmed = append(trimmed, stripped)
		}
		args = trimmed
	}

	if len(attachments) > MaxAttachments {
		return "", nil, errf("too many binary attachments (maximum %d)", MaxAttachments)
	}
	var b strings.Builder
	if base == Event || base == Ack {
		// the wire type gains a binary prefix only when lifting found bytes
		b.WriteByte(byte('0' + byte(base.withBinary(len(attachments) > 0))))
	} else {
		b.WriteByte(byte('0' + byte(base)))
	}
	if (base == Event || base == Ack) && len(attachments) > 0 {
		b.WriteString(strconv.Itoa(len(attachments)))
		b.WriteByte('-')
	}
	if p.Namespace != "" {
		b.WriteString(p.Namespace)
		b.WriteByte(',')
	}
	if p.HasID {
		b.WriteString(strconv.FormatInt(p.ID, 10))
	}

	if base == Event || base == Ack {
		if len(args) == 0 && base == Ack {
			// a zero-argument ack may omit the JSON entirely, which is what
			// the JS server emits
			return b.String(), attachments, nil
		}
		raw, err := json.Marshal(args)
		if err != nil {
			return "", nil, errf("cannot marshal args: %v", err)
		}
		b.Write(raw)
	} else if p.Data != nil {
		raw, err := json.Marshal(p.Data)
		if err != nil {
			return "", nil, errf("cannot marshal payload: %v", err)
		}
		b.Write(raw)
	}

	return b.String(), attachments, nil
}
