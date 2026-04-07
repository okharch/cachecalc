package smartcache

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func marshalValue(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	return buf.Bytes(), nil
}

func unmarshalValue(buf []byte, dest any) error {
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(dest); err != nil {
		return fmt.Errorf("decode value: %w", err)
	}
	return nil
}
