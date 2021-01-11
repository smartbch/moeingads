package fuzz

import (
	"testing"
)

// go test -tags cppbtree -c -coverpkg github.com/moeing-chain/MoeingADS/indextree .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=10000 ./fuzz.test -test.coverprofile a.out

func Test1(t *testing.T) {
	runTest(DefaultConfig)
}
