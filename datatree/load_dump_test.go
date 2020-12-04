package datatree

import (
	"os"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)


func compareNodes(t *testing.T, nodesA, nodesB map[NodePos]*[32]byte) {
	assert.Equal(t, len(nodesA), len(nodesB))
	for pos := range nodesA {
		assert.Equal(t, nodesA[pos], nodesB[pos])
	}
}

func compareTwigs(t *testing.T, twigMapA, twigMapB map[int64]*Twig) {
	assert.Equal(t, len(twigMapA), len(twigMapB))
	for id, twigA := range twigMapA {
		twigB := twigMapB[id]
		assert.Equal(t, twigA.activeBits, twigB.activeBits)
		assert.Equal(t, twigA.activeBitsMTL1, twigB.activeBitsMTL1)
		assert.Equal(t, twigA.activeBitsMTL2, twigB.activeBitsMTL2)
		assert.Equal(t, twigA.activeBitsMTL3, twigB.activeBitsMTL3)
		assert.Equal(t, twigA.leftRoot, twigB.leftRoot)
		assert.Equal(t, twigA.twigRoot, twigB.twigRoot)
		assert.Equal(t, twigA.FirstEntryPos, twigB.FirstEntryPos)
	}
}

func TestLoadTree(t *testing.T) {
	dirName := "./DataTree"
	os.RemoveAll(dirName)
	os.Mkdir(dirName, 0700)
	deactSNList := []int64{101, 999, 1002}
	tree0, _, _ := buildTestTree(dirName, deactSNList, TwigMask, 6)
	tree0.EndBlock()
	nodes0 := tree0.nodes
	activeTwigs0 := tree0.activeTwigs
	mtree4YoungestTwig0 := tree0.mtree4YoungestTwig
	tree0.Flush()
	tree0.Close()

	tree1 := LoadTree(SmallBufferSize, defaultFileSize, dirName)
	fmt.Printf("Load finished\n")
	compareNodes(t, tree1.nodes, nodes0)
	compareTwigs(t, tree1.activeTwigs, activeTwigs0)
	assert.Equal(t, tree1.mtree4YoungestTwig, mtree4YoungestTwig0)
	tree1.Close()

	tree2 := RecoverTree(SmallBufferSize, defaultFileSize, dirName, nil, 0, 0, 1)
	fmt.Printf("Recover finished\n")
	assert.Equal(t, tree2.mtree4YoungestTwig, mtree4YoungestTwig0)
	compareTwigs(t, tree2.activeTwigs, activeTwigs0)
	compareNodes(t, tree2.nodes, nodes0)
	tree2.Close()

	os.RemoveAll(dirName)
}
