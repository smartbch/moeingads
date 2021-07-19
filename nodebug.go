// +build !debug

package moeingads

import (
	"github.com/smartbch/moeingads/datatree"
)

const (
	DefaultHPFileSize  int   = 1024 * 1024 * 1024
	StartReapThres     int64 = 1000 * 1000
	HPFileBufferSize   int   = datatree.BufferSize
	RootCacheSizeLimit int   = 1024 * 1024
)

var DebugPanicNumber int //not used

func debugPanic(num int) {
	//do nothing
}
