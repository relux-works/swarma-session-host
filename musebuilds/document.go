package musebuilds

import (
	"encoding/json"
	"errors"
	"reflect"
)

// documentCodec is shared by each durable document's reader and writer.
// The bound includes JSON escaping and the terminal newline. Round-tripping
// before publication also refuses lossy JSON encoding (notably invalid UTF-8).
type documentCodec[T any] struct {
	bound    int64
	validate func(T) error
}

func (c documentCodec[T]) parse(raw []byte) (T, error) {
	var value T
	if err := decodeStrictJSON(raw, c.bound, &value); err != nil {
		return value, err
	}
	return value, c.validate(value)
}

func (c documentCodec[T]) encode(value T) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	decoded, err := c.parse(raw)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(decoded, value) {
		return nil, errors.New("serialized document does not preserve the supplied value")
	}
	return raw, nil
}

var (
	buildStateDocument = documentCodec[BuildState]{stateMaxBytes, validateBuildState}
	attemptDocument    = documentCodec[AttemptReceipt]{attemptReceiptMaxBytes, validateAttemptReceipt}
	lastGoodDocument   = documentCodec[LastGood]{lastGoodMaxBytes, validateLastGoodDocument}
)
