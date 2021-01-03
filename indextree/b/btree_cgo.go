// +build cppbtree

package b

import (
	"github.com/moeing-chain/MoeingADS/indextree/b/cppbtree"
)

type Enumerator = cppbtree.Enumerator

type Tree = cppbtree.Tree

func TreeNew() *Tree {
	return cppbtree.TreeNew()
}
