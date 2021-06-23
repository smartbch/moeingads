package b

import (
	"github.com/smartbch/moeingads/indextree/b/cppbtree"
)

type Enumerator = cppbtree.Enumerator

type Tree = cppbtree.Tree

func TreeNew() *Tree {
	return cppbtree.TreeNew()
}
