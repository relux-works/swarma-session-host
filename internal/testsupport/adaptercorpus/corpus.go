// Package adaptercorpus generates finite offline test descriptors. Only tests
// may import it; it adds no adapter behavior or native qualification.
package adaptercorpus

import (
	"encoding/json"
	"fmt"
)

type Case struct {
	ID, Array        string
	C, O, V, E, K, B int
}

func Generate() []Case {
	var out []Case
	for _, array := range []struct {
		name   string
		levels []int
	}{{"adapter", []int{3, 3, 3, 2, 4}}, {"host", []int{3, 3, 2, 2, 4, 4}}} {
		emit := func(v [6]int) {
			out = append(out, Case{ID: fmt.Sprintf("%s-c%d-o%d-v%d-e%d-k%d-b%d", array.name, v[0], v[1], v[2], v[3], v[4], v[5]), Array: array.name, C: v[0], O: v[1], V: v[2], E: v[3], K: v[4], B: v[5]})
		}
		emit([6]int{})
		for i, count := range array.levels {
			for a := 1; a < count; a++ {
				var v [6]int
				v[i] = a
				emit(v)
			}
		}
		for i := 0; i < len(array.levels); i++ {
			for j := i + 1; j < len(array.levels); j++ {
				for a := 1; a < array.levels[i]; a++ {
					for b := 1; b < array.levels[j]; b++ {
						var v [6]int
						v[i], v[j] = a, b
						emit(v)
					}
				}
			}
		}
	}
	return out
}

func (c Case) Vector() []int {
	v := []int{c.C, c.O, c.V, c.E, c.K, c.B}
	if c.Array == "adapter" {
		return v[:5]
	}
	return v
}

// Corrupt changes only the selected test-owned saved metadata. No validator is
// used to decide the expected verdict; the tests independently name each rule.
func Corrupt(saved []byte, kind int) ([]byte, error) {
	if kind == 0 {
		return append([]byte(nil), saved...), nil
	}
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(saved, &outer); err != nil {
		return nil, err
	}
	if kind == 1 {
		outer["version"] = json.RawMessage("99")
	} else {
		var stream map[string]json.RawMessage
		if err := json.Unmarshal(outer["stream"], &stream); err != nil {
			return nil, err
		}
		switch kind {
		case 2:
			stream["closed"] = json.RawMessage("null")
		case 3:
			var protocol map[string]json.RawMessage
			if err := json.Unmarshal(stream["protocol"], &protocol); err != nil {
				return nil, err
			}
			var request uint64
			if err := json.Unmarshal(protocol["request"], &request); err != nil {
				return nil, err
			}
			b, _ := json.Marshal(request + 1)
			protocol["pending"] = b
			b, err := json.Marshal(protocol)
			if err != nil {
				return nil, err
			}
			stream["protocol"] = b
		default:
			return nil, fmt.Errorf("unknown corruption %d", kind)
		}
		b, err := json.Marshal(stream)
		if err != nil {
			return nil, err
		}
		outer["stream"] = b
	}
	return json.Marshal(outer)
}

func Parts(b []byte, partition int) [][]byte {
	switch partition {
	case 0:
		return [][]byte{b}
	case 1:
		p := make([][]byte, 0, len(b))
		for i := range b {
			p = append(p, b[i:i+1])
		}
		return p
	case 2:
		if len(b) < 2 {
			return [][]byte{b}
		}
		return [][]byte{b[:len(b)-1], b[len(b)-1:]}
	}
	panic("invalid test partition")
}
