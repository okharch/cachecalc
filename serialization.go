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

func serializeEntry(e *CacheEntry) (result []byte, err error) {
	var errMsg string
	if e.Err != nil {
		errMsg = e.Err.Error()
	}
	ce := &ceSerialize{Expire: e.Expire, Refresh: e.Refresh, Error: errMsg, Value: e.Value}
	return serialize(ce)
}

func deserializeEntry(buf []byte, entry *CacheEntry) (err error) {
	var e ceSerialize
	err = deserialize(buf, &e)
	if err != nil {
		return
	}
	entry.Expire = e.Expire
	entry.Refresh = e.Refresh
	if e.Error != "" {
		entry.Err = errors.New(e.Error)
	}
	entry.Value = e.Value
	return nil
}

func DeserializeValue(buf []byte, valPtr any) error {
	return deserialize(buf, valPtr)
}
