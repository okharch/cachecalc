//go:build windows
// +build windows

package cachecalc

import "time"

// tick is the minimum delay which is used for testing purposes. for windows, it is 20ms
const tick = time.Millisecond * 20
