package cachecalc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"time"
)

func serialize(value any) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(value)
	if err == nil {
		return buf.Bytes(), nil
	}
	return nil, err
}

func deserialize(d []byte, valPtr any) error {
	dec := gob.NewDecoder(bytes.NewBuffer(d))
	err := dec.Decode(valPtr)
	if err != nil {
		err = fmt.Errorf("deserialize value error: %w", err)
	}
	return err
}

type ceSerialize struct {
	Expire  time.Time
	Refresh time.Time
	Error   string
	Value   []byte
}

func serializeEntry(e *cacheEntry) (result []byte, err error) {
	ce := &ceSerialize{Expire: e.Expire, Refresh: e.Refresh, Error: e.Err.Error(), Value: e.Value}
	return serialize(ce)
}

func deserializeEntry(buf []byte, dest any) (result *cacheEntry, err error) {
	var e ceSerialize
	err = deserialize(buf, &e)
	if err != nil {
		return
	}
	result = &cacheEntry{Expire: e.Expire, Refresh: e.Refresh, Err: errors.New(e.Error), Value: e.Value}
	err = deserialize(e.Value, dest)
	return
}
