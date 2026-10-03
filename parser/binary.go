package parser

import (
	"encoding"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// liftBinaries supports typed Go containers as well as decoded JSON values.
// Custom JSON marshalers retain control of their representation.
func liftBinaries(node any, attachments *[][]byte, depth int) (any, error) {
	return liftBinaryValue(reflect.ValueOf(node), attachments, depth)
}

func liftBinaryValue(v reflect.Value, attachments *[][]byte, depth int) (any, error) {
	if depth > 32 {
		return nil, errf("args nested too deeply")
	}
	if !v.IsValid() || v.Kind() == reflect.Interface && v.IsNil() {
		return nil, nil
	}
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, nil
	}
	node := v.Interface()
	if _, custom := node.(json.Marshaler); custom {
		return node, nil
	}
	if _, custom := node.(encoding.TextMarshaler); custom {
		return node, nil
	}
	if v.CanAddr() && v.Addr().CanInterface() {
		pointer := v.Addr().Interface()
		if _, custom := pointer.(json.Marshaler); custom {
			return pointer, nil
		}
		if _, custom := pointer.(encoding.TextMarshaler); custom {
			return pointer, nil
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return liftBinaryValue(v.Elem(), attachments, depth+1)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			if len(*attachments) >= MaxAttachments {
				return nil, errf("too many binary attachments (maximum %d)", MaxAttachments)
			}
			num := len(*attachments)
			*attachments = append(*attachments, v.Bytes())
			return map[string]any{"_placeholder": true, "num": num}, nil
		}
		if v.IsNil() {
			return node, nil
		}
		fallthrough
	case reflect.Array:
		out := make([]any, v.Len())
		for i := range out {
			item, err := liftBinaryValue(v.Index(i), attachments, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = item
		}
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return node, nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			key, err := jsonMapKey(iter.Key())
			if err != nil {
				return nil, err
			}
			item, err := liftBinaryValue(iter.Value(), attachments, depth+1)
			if err != nil {
				return nil, err
			}
			out[key] = item
		}
		return out, nil
	case reflect.Struct:
		return liftStruct(v, attachments, depth)
	}
	return node, nil
}

func jsonMapKey(v reflect.Value) (string, error) {
	if v.Kind() == reflect.String {
		return v.String(), nil
	}
	if key, ok := v.Interface().(encoding.TextMarshaler); ok {
		text, err := key.MarshalText()
		return string(text), err
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10), nil
	}
	return "", errf("unsupported JSON map key %s", v.Type())
}

type binaryField struct {
	name   string
	index  []int
	tagged bool
}

// Marshal first so encoding/json decides omission, quoting and field visibility.
// Replace only fields that actually contain binary; preserve all other raw JSON
// (including integer precision). Resolve embedded fields by depth and tag priority.
func liftStruct(v reflect.Value, attachments *[][]byte, depth int) (any, error) {
	original := v.Interface()
	if v.CanAddr() && v.Addr().CanInterface() {
		original = v.Addr().Interface()
	}
	raw, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		out[key] = value
	}
	candidates := map[string][]binaryField{}
	collectBinaryFields(v.Type(), nil, map[reflect.Type]bool{}, candidates)
	for name, choices := range candidates {
		if _, included := fields[name]; !included {
			continue
		}
		sort.Slice(choices, func(i, j int) bool {
			if len(choices[i].index) != len(choices[j].index) {
				return len(choices[i].index) < len(choices[j].index)
			}
			return choices[i].tagged && !choices[j].tagged
		})
		if len(choices) > 1 && len(choices[0].index) == len(choices[1].index) && choices[0].tagged == choices[1].tagged {
			continue // ambiguous fields are omitted by encoding/json
		}
		value := v
		for _, index := range choices[0].index {
			if value.Kind() == reflect.Pointer {
				if value.IsNil() {
					break
				}
				value = value.Elem()
			}
			value = value.Field(index)
		}
		if !value.CanInterface() {
			continue
		}
		before := len(*attachments)
		lifted, err := liftBinaryValue(value, attachments, depth+1)
		if err != nil {
			return nil, err
		}
		if len(*attachments) != before {
			out[name] = lifted
		}
	}
	return out, nil
}

func collectBinaryFields(t reflect.Type, prefix []int, visiting map[reflect.Type]bool, fields map[string][]binaryField) {
	if visiting[t] || len(prefix) > 32 {
		return
	}
	visiting[t] = true
	defer delete(visiting, t)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		ft := field.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if field.PkgPath != "" && (!field.Anonymous || ft.Kind() != reflect.Struct) {
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "-" {
			continue
		}
		index := append(append([]int(nil), prefix...), i)
		if field.Anonymous && name == "" && ft.Kind() == reflect.Struct {
			collectBinaryFields(ft, index, visiting, fields)
			continue
		}
		tagged := name != ""
		if name == "" {
			name = field.Name
		}
		fields[name] = append(fields[name], binaryField{name: name, index: index, tagged: tagged})
	}
}
