// +build cppbtree

package b

import (
	"github.com/moeing-chain/MoeingADS/indextree/b/cppbtree"
)

type Enumerator = cppbtree.Enumerator

type Tree = cppbtree.Tree

func TreeNew(fn func(a, b []byte) int) *Tree {
	return cppbtree.TreeNew(fn)
}
