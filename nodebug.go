// +build !debug

package moeingads

const (
	defaultFileSize                   int = 1024 * 1024 * 1024
	StartReapThres                  int64 = 1000 * 1000
)

var DebugPanicNumber int //not used

func debugPanic(num int) {
	//do nothing
}
