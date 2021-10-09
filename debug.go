//go:build debug
// +build debug

package moeingads

import (
	"fmt"
)

const (
	DefaultHPFileSize  int   = 64 * 1024 * 1024
	StartReapThres     int64 = 10 * 1000
	HPFileBufferSize   int   = 8 * 1024
	RootCacheSizeLimit int   = 1024
)

var DebugPanicNumber int

func debugPanic(num int) {
	if num == DebugPanicNumber {
		panic(fmt.Sprintf("panic for debug, num=%d", num))
	}
}
