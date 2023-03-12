package cachecalc

import (
	"bytes"
	"crypto/rand"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSerialize(t *testing.T) {
	//sc := NewCache(ctx, 5, nil)
	var wg sync.WaitGroup
	const concurrent = 100
	test := func() {
		defer wg.Done()
		const v1 = "test"
		b, err := serialize(v1)
		require.NoError(t, err)
		require.NotNil(t, b)
		var v1Got string
		err = deserialize(b, &v1Got)
		require.NoError(t, err)
		require.Equal(t, v1, v1Got)
		// some more complex stuff
		v2 := make([]byte, 16)
		_, err = rand.Read(v2)
		require.NoError(t, err)
		b, err = serialize(v2)
		require.NoError(t, err)
		require.NotNil(t, b)
		//var resb []byte
		//e1.Value = &res
		var v2got []byte
		err = deserialize(b, &v2got)
		require.NoError(t, err)
		require.Equal(t, v2, v2got)
	}
	wg.Add(concurrent)
	test()
	test()
	for i := 2; i < concurrent; i++ {
		go test()
	}
	//wg.Wait()
}

var someErr = errors.New("Some error")

func testSerializeType(t *testing.T, val any, dest any, compare func() bool) {
	now := time.Now()
	buf, err := serialize(val)
	require.NoError(t, err)
	ce := &cacheEntry{Refresh: now, Expire: now, Err: someErr, Value: buf}
	require.Equal(t, someErr, ce.Err)
	buf, err = serializeEntry(ce)
	require.NoError(t, err)
	require.NotNil(t, buf)
	var ce1 cacheEntry
	err = deserializeEntry(buf, &ce1)
	require.NoError(t, err)
	require.True(t, ce.Expire.Equal(ce1.Expire))
	require.True(t, ce.Refresh.Equal(ce1.Refresh))
	require.Equal(t, ce.Err, ce1.Err)
	err = deserialize(ce1.Value, dest)
	require.NoError(t, err)
	require.True(t, compare())
}

func TestCacheEntrySerialize(t *testing.T) {
	var str string
	testSerializeType(t, "Hello", &str, func() bool { return str == "Hello" })
	cbytes := []byte{1, 2, 3, 4}
	var gbytes []byte
	testSerializeType(t, cbytes, &gbytes, func() bool { return bytes.Equal(cbytes, gbytes) })
}

func TestSerializeEmptyEntry(t *testing.T) {
	var ce cacheEntry
	buf, err := serializeEntry(&ce)
	require.NoError(t, err)
	require.NotNil(t, buf)
	var ce1 cacheEntry
	err = deserializeEntry(buf, &ce1)
	require.NoError(t, err)
	require.True(t, ce.Expire.Equal(ce1.Expire))
	require.True(t, ce.Refresh.Equal(ce1.Refresh))
	require.Equal(t, ce.Err, ce1.Err)
	require.NoError(t, ce1.Err)
	require.Nil(t, ce1.Value)
}
