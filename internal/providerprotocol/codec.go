package providerprotocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Frame is a proposed control envelope, unrelated to the notification method
// wire. ID is a nonzero operation sequence; callbacks use the enclosing ID.
// The four-byte big-endian length excludes its own header.
type Frame struct {
	Version uint16          `json:"version"`
	ID      uint32          `json:"id"`
	Type    string          `json:"type"`
	Body    json.RawMessage `json:"body,omitempty"`
}

func NewFrame(id uint32, kind string, body any) (Frame, error) {
	f := Frame{Version: Version, ID: id, Type: kind}
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return Frame{}, ErrFrame
		}
		f.Body = b
	}
	if err := f.validate(); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// strict rejects duplicate keys (including escaped aliases), null, excessive
// nesting, invalid UTF-8, unknown fields and multiple values at every level.
func strict(b []byte, out any) error {
	if !utf8.Valid(b) {
		return ErrFrame
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := value(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrFrame
	}
	if err := keys(b, reflect.TypeOf(out).Elem()); err != nil {
		return err
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrFrame
	}
	return nil
}

var rawType = reflect.TypeOf(json.RawMessage{})

func keys(b []byte, t reflect.Type) error {
	if t == rawType || t.Kind() != reflect.Struct {
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || fields == nil {
		return ErrFrame
	}
	allowed := make(map[string]bool)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")
		name := tag[0]
		allowed[name] = true
		v, ok := fields[name]
		if !ok {
			if len(tag) < 2 || tag[1] != "omitempty" {
				return ErrFrame
			}
			continue
		}
		if err := keys(v, f.Type); err != nil {
			return err
		}
	}
	for name := range fields {
		if !allowed[name] {
			return ErrFrame
		}
	}
	return nil
}
func value(d *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrFrame
	}
	t, err := d.Token()
	if err != nil || t == nil {
		return ErrFrame
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return ErrFrame
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return ErrFrame
				}
				seen[s] = true
				if err := value(d, depth+1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return ErrFrame
			}
		case '[':
			// No accepted control DTO contains an array.
			return ErrFrame
		default:
			return ErrFrame
		}
	}
	return nil
}

func (f Frame) validate() error {
	if f.Version != Version {
		return ErrVersion
	}
	if f.ID == 0 || len(f.Body) > MaxFrame {
		return ErrFrame
	}
	switch f.Type {
	case "hello", "hello_ok", "recheck", "rechecked", "recheck_host", "host_checked", "installed", "ack", "close", "closed":
		if len(f.Body) != 0 {
			return ErrFrame
		}
	case "prepare":
		var p PrepareRequest
		if strict(f.Body, &p) != nil || !p.valid() {
			return ErrFrame
		}
	case "prepared":
		var r References
		if strict(f.Body, &r) != nil || !r.valid() {
			return ErrFrame
		}
	case "bootstrap":
		var b BootstrapRequest
		if strict(f.Body, &b) != nil || !b.valid() {
			return ErrFrame
		}
	case "enrollment":
		var e Enrollment
		if strict(f.Body, &e) != nil || !e.valid() {
			return ErrFrame
		}
	case "refused":
		var r Refusal
		if strict(f.Body, &r) != nil || !r.valid() {
			return ErrFrame
		}
	default:
		return ErrFrame
	}
	return nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		if err == io.EOF {
			return Frame{}, ErrUnresolved
		}
		return Frame{}, ErrFrame
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > MaxFrame {
		return Frame{}, ErrFrame
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(r, b); err != nil {
		return Frame{}, ErrFrame
	}
	var f Frame
	if strict(b, &f) != nil {
		return Frame{}, ErrFrame
	}
	if err := f.validate(); err != nil {
		return Frame{}, err
	}
	return f, nil
}

func WriteFrame(w io.Writer, f Frame) error {
	if err := f.validate(); err != nil {
		return err
	}
	b, err := json.Marshal(f)
	if err != nil || len(b) > MaxFrame {
		return ErrFrame
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	for _, part := range [][]byte{h[:], b} {
		for len(part) > 0 {
			n, err := w.Write(part)
			if err != nil || n <= 0 || n > len(part) {
				return ErrUnresolved
			}
			part = part[n:]
		}
	}
	return nil
}
