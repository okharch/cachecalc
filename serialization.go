package cachecalc

import (
	"bytes"
	"encoding/gob"
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
	return dec.Decode(valPtr)
}

type ceSerialize struct {
	Expire  time.Time
	Refresh time.Time
	Error   string
	Value   []byte
}

func serialize_entry(e *cacheEntry) (result []byte, err error) {
	ce := &ceSerialize{Expire: e.Expire, Refresh: e.Refresh, Error: e.Err, Value: e.Value}
	return serialize(ce)
}

func deserialize_entry(buf []byte, dest any) (result *cacheEntry, err error) {
	var e ceSerialize
	err = deserialize(buf, &e)
	if err != nil {
		return
	}
	result = &cacheEntry{Expire: e.Expire, Refresh: e.Refresh, Err: e.Error, Value: e.Value}
	err = deserialize(e.Value, dest)
	return
}
