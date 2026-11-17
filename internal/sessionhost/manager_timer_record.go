package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"sort"
	"strconv"
	"unicode/utf8"
)

const (
	managerTimerProtocol   = "host-manager-timer/1"
	managerTimerFrameLimit = 65536
)

type managerTimerCode string

func (code managerTimerCode) Error() string { return string(code) }

const (
	managerTimerInvalid       managerTimerCode = "timer_registration_invalid"
	managerTimerFrameTooLarge managerTimerCode = "timer_frame_too_large"
)

// Decoding/canonicalization is an effect-free data operation, not an admission
// decision. Typed record validation and the ordinary owner's current proof must
// precede any timer intent. Notification framing cannot be reused here: the
// timer record language permits JSON whitespace, nulls and bounded arrays.
func decodeManagerTimerObject(frame []byte) (map[string]any, error) {
	return decodeManagerTimerObjectLimit(frame, managerTimerFrameLimit)
}

// The private journal has a distinct bound; increasing it never changes a wire
// frame's limit. Both paths retain the same exact integer and Unicode language.
func decodeManagerTimerObjectLimit(frame []byte, limit int) (map[string]any, error) {
	if len(frame) > limit {
		return nil, managerTimerFrameTooLarge
	}
	if len(frame) < 3 || frame[len(frame)-1] != '\n' || !utf8.Valid(frame) || !managerTimerUnicodeValid(frame) {
		return nil, managerTimerInvalid
	}
	d := json.NewDecoder(bytes.NewReader(frame))
	d.UseNumber()
	value, err := decodeManagerTimerValue(d, 0)
	if err != nil {
		return nil, managerTimerInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, managerTimerInvalid
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, managerTimerInvalid
	}
	return object, nil
}

func decodeManagerTimerValue(d *json.Decoder, depth int) (any, error) {
	if depth > 32 {
		return nil, managerTimerInvalid
	}
	token, err := d.Token()
	if err != nil {
		return nil, managerTimerInvalid
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			object := make(map[string]any)
			for d.More() {
				token, err := d.Token()
				key, ok := token.(string)
				if err != nil || !ok {
					return nil, managerTimerInvalid
				}
				if _, exists := object[key]; exists {
					return nil, managerTimerInvalid
				}
				value, err := decodeManagerTimerValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if end, err := d.Token(); err != nil || end != json.Delim('}') {
				return nil, managerTimerInvalid
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for d.More() {
				value, err := decodeManagerTimerValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			if end, err := d.Token(); err != nil || end != json.Delim(']') {
				return nil, managerTimerInvalid
			}
			return array, nil
		default:
			return nil, managerTimerInvalid
		}
	case json.Number:
		// ParseInt rejects fraction/exponent notation without a float
		// intermediary; -0 is an exact integer and canonicalizes to 0.
		n, err := strconv.ParseInt(string(token), 10, 64)
		if err != nil || n < 0 {
			return nil, managerTimerInvalid
		}
		return json.Number(strconv.FormatInt(n, 10)), nil
	case nil, bool, string:
		return token, nil
	default:
		return nil, managerTimerInvalid
	}
}

// encoding/json replaces unpaired surrogate escapes, so reject them in the
// original bytes before Token can erase that distinction. Syntax is still
// checked by the JSON decoder; this scan only preserves Unicode validity.
func managerTimerUnicodeValid(data []byte) bool {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		} else if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
	}
	return !quoted
}

func canonicalManagerTimerValue(value any) ([]byte, error) {
	var out bytes.Buffer
	var write func(any, int) error
	writeString := func(s string) error {
		if !utf8.ValidString(s) {
			return managerTimerInvalid
		}
		out.WriteByte('"')
		const digits = "0123456789abcdef"
		for _, r := range s {
			switch {
			case r == '"' || r == '\\':
				out.WriteByte('\\')
				out.WriteRune(r)
			case r < 32:
				out.WriteString(`\u00`)
				out.WriteByte(digits[byte(r)>>4])
				out.WriteByte(digits[byte(r)&15])
			default:
				out.WriteRune(r)
			}
		}
		out.WriteByte('"')
		return nil
	}
	write = func(value any, depth int) error {
		if depth > 32 {
			return managerTimerInvalid
		}
		switch value := value.(type) {
		case nil:
			out.WriteString("null")
		case bool:
			out.WriteString(strconv.FormatBool(value))
		case string:
			return writeString(value)
		case json.Number:
			n, ok := managerTimerInteger(value)
			if !ok {
				return managerTimerInvalid
			}
			out.WriteString(strconv.FormatInt(n, 10))
		case []any:
			out.WriteByte('[')
			for i, item := range value {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := write(item, depth+1); err != nil {
					return err
				}
			}
			out.WriteByte(']')
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			out.WriteByte('{')
			for i, key := range keys {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := writeString(key); err != nil {
					return err
				}
				out.WriteByte(':')
				if err := write(value[key], depth+1); err != nil {
					return err
				}
			}
			out.WriteByte('}')
		default:
			return managerTimerInvalid
		}
		return nil
	}
	if err := write(value, 0); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func managerTimerDomainDigest(kind string, value any) ([32]byte, error) {
	data, err := canonicalManagerTimerValue(value)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(managerTimerProtocol+":"+kind+"\n"), data...)), nil
}
