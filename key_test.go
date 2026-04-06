package cachecalc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeKeyIncludesTypeInformation(t *testing.T) {
	k1, err := MakeKey(1)
	require.NoError(t, err)

	k2, err := MakeKey("1")
	require.NoError(t, err)

	require.NotEqual(t, k1, k2)
}

func TestMakeKeyReturnsErrorForUnsupportedValue(t *testing.T) {
	_, err := MakeKey(func() {})
	require.Error(t, err)
}
