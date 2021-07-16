// +build !debug

package moeingads

import (
	"github.com/smartbch/moeingads/datatree"
)

const (
	defaultFileSize int   = 1024 * 1024 * 1024
	StartReapThres  int64 = 1000 * 1000
	bufferSize      int   = datatree.BufferSize
)

var DebugPanicNumber int //not used

func debugPanic(num int) {
	//do nothing
}
