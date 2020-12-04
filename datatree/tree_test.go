package datatree

import (
	"os"
	"encoding/binary"
	"fmt"
	"testing"

	sha256 "github.com/minio/sha256-simd"
	"github.com/stretchr/testify/assert"
)

// test the init function in tree.go
func TestTreeInit(t *testing.T) {
	var L8,L9,L10,L11 [32]byte
	L9  = sha256.Sum256(append(append([]byte{8}, L8[:]...), L8[:]...))
	L10 = sha256.Sum256(append(append([]byte{9}, L9[:]...), L9[:]...))
	L11 = sha256.Sum256(append(append([]byte{10}, L10[:]...), L10[:]...))

	assert.Equal(t, L9,  NullTwig.activeBitsMTL1[0])
	assert.Equal(t, L9,  NullTwig.activeBitsMTL1[1])
	assert.Equal(t, L9,  NullTwig.activeBitsMTL1[2])
	assert.Equal(t, L9,  NullTwig.activeBitsMTL1[3])
	assert.Equal(t, L10, NullTwig.activeBitsMTL2[0])
	assert.Equal(t, L10, NullTwig.activeBitsMTL2[1])
	assert.Equal(t, L11, NullTwig.activeBitsMTL3)

	var lvl [21][32]byte
	lvl[0] = sha256.Sum256(EntryToBytes(NullEntry(), nil))
	lvl[1] = sha256.Sum256(append(append([]byte{0}, lvl[0][:]...), lvl[0][:]...))
	lvl[2] = sha256.Sum256(append(append([]byte{1}, lvl[1][:]...), lvl[1][:]...))
	lvl[3] = sha256.Sum256(append(append([]byte{2}, lvl[2][:]...), lvl[2][:]...))
	lvl[4] = sha256.Sum256(append(append([]byte{3}, lvl[3][:]...), lvl[3][:]...))
	lvl[5] = sha256.Sum256(append(append([]byte{4}, lvl[4][:]...), lvl[4][:]...))
	lvl[6] = sha256.Sum256(append(append([]byte{5}, lvl[5][:]...), lvl[5][:]...))
	lvl[7] = sha256.Sum256(append(append([]byte{6}, lvl[6][:]...), lvl[6][:]...))
	lvl[8] = sha256.Sum256(append(append([]byte{7}, lvl[7][:]...), lvl[7][:]...))
	lvl[9] = sha256.Sum256(append(append([]byte{8}, lvl[8][:]...), lvl[8][:]...))
	lvl[10] = sha256.Sum256(append(append([]byte{9}, lvl[9][:]...), lvl[9][:]...))
	lvl[11] = sha256.Sum256(append(append([]byte{10}, lvl[10][:]...), lvl[10][:]...))
	lvl[12] = sha256.Sum256(append(append([]byte{11}, lvl[11][:]...), NullTwig.activeBitsMTL3[:]...))
	lvl[13] = sha256.Sum256(append(append([]byte{12}, lvl[12][:]...), lvl[12][:]...))
	lvl[14] = sha256.Sum256(append(append([]byte{13}, lvl[13][:]...), lvl[13][:]...))
	lvl[15] = sha256.Sum256(append(append([]byte{14}, lvl[14][:]...), lvl[14][:]...))
	lvl[16] = sha256.Sum256(append(append([]byte{15}, lvl[15][:]...), lvl[15][:]...))
	lvl[17] = sha256.Sum256(append(append([]byte{16}, lvl[16][:]...), lvl[16][:]...))
	lvl[18] = sha256.Sum256(append(append([]byte{17}, lvl[17][:]...), lvl[17][:]...))
	lvl[19] = sha256.Sum256(append(append([]byte{18}, lvl[18][:]...), lvl[18][:]...))
	lvl[20] = sha256.Sum256(append(append([]byte{19}, lvl[19][:]...), lvl[19][:]...))
	for i, stripe := 0, 2048; i < stripe; i++ {
		assert.Equal(t, lvl[0], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 1024; i < stripe; i++ {
		assert.Equal(t, lvl[1], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 512; i < stripe; i++ {
		assert.Equal(t, lvl[2], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 256; i < stripe; i++ {
		assert.Equal(t, lvl[3], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 128; i < stripe; i++ {
		assert.Equal(t, lvl[4], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 64; i < stripe; i++ {
		assert.Equal(t, lvl[5], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 32; i < stripe; i++ {
		assert.Equal(t, lvl[6], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 16; i < stripe; i++ {
		assert.Equal(t, lvl[7], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 8; i < stripe; i++ {
		assert.Equal(t, lvl[8], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 4; i < stripe; i++ {
		assert.Equal(t, lvl[9], NullMT4Twig[stripe+i])
	}
	for i, stripe := 0, 2; i < stripe; i++ {
		assert.Equal(t, lvl[10], NullMT4Twig[stripe+i])
	}
	assert.Equal(t, lvl[11], NullMT4Twig[1])
	assert.Equal(t, lvl[11], NullTwig.leftRoot)
	assert.Equal(t, lvl[12], NullTwig.twigRoot)
	assert.Equal(t, lvl[13], NullNodeInHigherTree[13])
	assert.Equal(t, lvl[14], NullNodeInHigherTree[14])
	assert.Equal(t, lvl[15], NullNodeInHigherTree[15])
	assert.Equal(t, lvl[16], NullNodeInHigherTree[16])
	assert.Equal(t, lvl[17], NullNodeInHigherTree[17])
	assert.Equal(t, lvl[18], NullNodeInHigherTree[18])
	assert.Equal(t, lvl[19], NullNodeInHigherTree[19])
	assert.Equal(t, lvl[20], NullNodeInHigherTree[20])
}

// test getBit, setBit and clearBit of twig
func TestTreeTwig(t *testing.T) {
	twig := &Twig{}
	for i := 0; i < 2048; i++ {
		assert.Equal(t, false, twig.getBit(i))
	}
	ones := []int{1,2,3, 54, 199, 200, 29, 37, 1000, 2000, 2008, 799, 2045, 2046, 2047}
	for _, i := range ones {
		twig.setBit(i)
	}
	for _, i := range ones {
		assert.Equal(t, true, twig.getBit(i))
	}
	for _, i := range ones {
		twig.clearBit(i)
	}
	for _, i := range ones {
		assert.Equal(t, false, twig.getBit(i))
	}
}

// Test EdgeNodes' serialization and deserialization
func TestTreeEdgeNode(t *testing.T) {
	en0 := &EdgeNode{
		Pos:   Pos(1, 1000),
		Value: make([]byte, 32),
	}
	en1 := &EdgeNode{
		Pos:   Pos(12, 31000),
		Value: make([]byte, 32),
	}
	en2 := &EdgeNode{
		Pos:   Pos(31, 100380),
		Value: make([]byte, 32),
	}
	for i := 0; i < 32; i++ {
		en0.Value[i] = 10
		en1.Value[i] = 11
		en2.Value[i] = 12
	}
	bz := EdgeNodesToBytes([]*EdgeNode{en0,en1,en2})
	edgeNodes := BytesToEdgeNodes(bz)
	assert.Equal(t, 3, len(edgeNodes))
	assert.Equal(t, en0, edgeNodes[0])
	assert.Equal(t, en1, edgeNodes[1])
	assert.Equal(t, en2, edgeNodes[2])
}

func TestTreeMaxLevel(t *testing.T) {
	assert.Equal(t, 12, calcMaxLevel(0))
	assert.Equal(t, 13, calcMaxLevel(1))
	assert.Equal(t, 14, calcMaxLevel(2))
	assert.Equal(t, 14, calcMaxLevel(3))
	assert.Equal(t, 15, calcMaxLevel(4))
	assert.Equal(t, 15, calcMaxLevel(7))
	assert.Equal(t, 16, calcMaxLevel(8))
	assert.Equal(t, 16, calcMaxLevel(15))
	assert.Equal(t, 17, calcMaxLevel(16))
	assert.Equal(t, 18, calcMaxLevel(32))
	assert.Equal(t, 19, calcMaxLevel(64))
	assert.Equal(t, 20, calcMaxLevel(128))
	assert.Equal(t, 21, calcMaxLevel(256))
	assert.Equal(t, 21, calcMaxLevel(511))
}

/*
                 |0                17
        |0               .1        16
   |0        1      .2       3     15
|0   |1   2   3  .4   .5   6   7   14
0 1|2 *3 4 5 6 7 8 9.a *b c d e f  13
*/
// fill tree.nodes and test tree.ReapNodes
func TestTreeReapNodes(t *testing.T) {
	tree := &Tree{nodes: make(map[NodePos]*[32]byte)}
	stripe := 32
	for level := FirstLevelAboveTwig-1; level < FirstLevelAboveTwig+5; level++ {
		for i := 0; i < stripe; i++ {
			var zero [32]byte
			tree.nodes[Pos(int(level), int64(i))] = &zero
		}
		stripe >>= 1
	}
	tree.youngestTwigID = 15
	bz := tree.ReapNodes(0, 3)
	edgeNodes := BytesToEdgeNodes(bz)
	assert.Equal(t, 5, len(edgeNodes))
	var zero [32]byte
	assert.Equal(t, &EdgeNode{Pos: Pos(12, 3), Value: zero[:]}, edgeNodes[0])
	assert.Equal(t, &EdgeNode{Pos: Pos(13, 0), Value: zero[:]}, edgeNodes[1])
	assert.Equal(t, &EdgeNode{Pos: Pos(14, 0), Value: zero[:]}, edgeNodes[2])
	assert.Equal(t, &EdgeNode{Pos: Pos(15, 0), Value: zero[:]}, edgeNodes[3])
	assert.Equal(t, &EdgeNode{Pos: Pos(16, 0), Value: zero[:]}, edgeNodes[4])
	bz = tree.ReapNodes(3, 4)
	edgeNodes = BytesToEdgeNodes(bz)
	assert.Equal(t, 5, len(edgeNodes))
	assert.Equal(t, &EdgeNode{Pos: Pos(12, 4), Value: zero[:]}, edgeNodes[0])
	assert.Equal(t, &EdgeNode{Pos: Pos(13, 2), Value: zero[:]}, edgeNodes[1])
	assert.Equal(t, &EdgeNode{Pos: Pos(14, 0), Value: zero[:]}, edgeNodes[2])
	assert.Equal(t, &EdgeNode{Pos: Pos(15, 0), Value: zero[:]}, edgeNodes[3])
	assert.Equal(t, &EdgeNode{Pos: Pos(16, 0), Value: zero[:]}, edgeNodes[4])
	bz = tree.ReapNodes(4, 22)
	edgeNodes = BytesToEdgeNodes(bz)
	//for _, edgeNode := range edgeNodes {
	//	pos := int64(edgeNode.Pos)
	//	fmt.Printf("Pos(%d, %d) %#v\n", pos>>56, (pos<<8)>>8, edgeNode.Value)
	//}
	assert.Equal(t, &EdgeNode{Pos: Pos(12, 22), Value: zero[:]}, edgeNodes[0])
	assert.Equal(t, &EdgeNode{Pos: Pos(13, 10), Value: zero[:]}, edgeNodes[1])
	assert.Equal(t, &EdgeNode{Pos: Pos(14, 4), Value: zero[:]}, edgeNodes[2])
	assert.Equal(t, &EdgeNode{Pos: Pos(15, 2), Value: zero[:]}, edgeNodes[3])
	assert.Equal(t, &EdgeNode{Pos: Pos(16, 0), Value: zero[:]}, edgeNodes[4])

	for i := 23; i < 32; i++ {
		assert.Equal(t, zero, *tree.nodes[Pos(12, int64(i))])
	}
	assert.Equal(t, zero, *tree.nodes[Pos(13, 10)])
	assert.Equal(t, zero, *tree.nodes[Pos(13, 11)])
	assert.Equal(t, zero, *tree.nodes[Pos(13, 12)])
	assert.Equal(t, zero, *tree.nodes[Pos(13, 13)])
	assert.Equal(t, zero, *tree.nodes[Pos(13, 14)])
	assert.Equal(t, zero, *tree.nodes[Pos(13, 15)])
	assert.Equal(t, zero, *tree.nodes[Pos(14, 4)])
	assert.Equal(t, zero, *tree.nodes[Pos(14, 5)])
	assert.Equal(t, zero, *tree.nodes[Pos(14, 6)])
	assert.Equal(t, zero, *tree.nodes[Pos(14, 7)])
	assert.Equal(t, zero, *tree.nodes[Pos(15, 2)])
	assert.Equal(t, zero, *tree.nodes[Pos(15, 3)])
	assert.Equal(t, zero, *tree.nodes[Pos(16, 0)])
	assert.Equal(t, zero, *tree.nodes[Pos(16, 1)])
	assert.Equal(t, zero, *tree.nodes[Pos(17, 0)])
	for n := range tree.nodes {
		fmt.Printf("%d %d\n", int64(n)>>56, (int64(n)<<8)>>8)
	}
	assert.Equal(t, 25, len(tree.nodes))

}

func generateMT4YoungestTwig() (mt [4096][32]byte) {
	for i := range mt {
		for j := 0; j < 32; j+=2 {
			binary.LittleEndian.PutUint16(mt[i][j:j+2], uint16(i))
		}
	}
	return
}

func changeRangeInMT4YoungestTwig(tree *Tree, start, end int) {
	for i := start; i <= end; i++ {
		//fmt.Printf("changeAt: %d(%d)\n", i, 2048+i)
		for j := range tree.mtree4YoungestTwig[2048+i] {
			tree.mtree4YoungestTwig[2048+i][j]++
		}
	}
	tree.mtree4YTChangeStart = start
	tree.mtree4YTChangeEnd = end
}

func TestTreeSyncMT4YoungestTwig(t *testing.T) {
	tree := &Tree{
		activeTwigs:    make(map[int64]*Twig),
		youngestTwigID: 0,
	}
	tree.activeTwigs[tree.youngestTwigID] = CopyNullTwig()
	tree.mtree4YoungestTwig = generateMT4YoungestTwig()
	tree.mtree4YTChangeStart = 0
	tree.mtree4YTChangeEnd = 2047
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 0, 0)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 2047, 2047)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 0, 1)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 2046, 2047)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 10, 11)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)

	changeRangeInMT4YoungestTwig(tree, 101, 1100)
	tree.syncMT4YoungestTwig()
	checkMT(tree.mtree4YoungestTwig)
}

func initNTwigs(tree *Tree, n int64) {
	tree.activeTwigs = make(map[int64]*Twig)
	for i := int64(0); i < n; i++ {
		tree.activeTwigs[i] = CopyNullTwig()
	}
	tree.youngestTwigID = n - 1
}

func flipActiveBitsEveryN(tree *Tree, n int64) {
	end := (1 + tree.youngestTwigID) * LeafCountInTwig
	for i := int64(0); i < end; i += n {
		active := tree.GetActiveBit(i)
		tree.setEntryActiviation(i, !active)
	}
}

func TestTreeSyncMT4ActiveBits(t *testing.T) {
	tree := &Tree{}
	initNTwigs(tree, 7)
	checkAllTwigs(tree)

	nList0 := []int64{0, 1, 2, 3}
	nList1 := []int64{0, 1, 2}
	nList2 := []int64{0, 2}

	tree.touchedPosOf512b = make(map[int64]struct{})
	flipActiveBitsEveryN(tree, 257)
	nList := tree.syncMT4ActiveBits()
	assert.Equal(t, nList0, nList)
	fmt.Printf("here0 %#v\n", nList)
	checkAllTwigs(tree)

	tree.touchedPosOf512b = make(map[int64]struct{})
	flipActiveBitsEveryN(tree, 600)
	nList = tree.syncMT4ActiveBits()
	assert.Equal(t, nList0, nList)
	fmt.Printf("here1 %#v\n", nList)
	checkAllTwigs(tree)

	tree.touchedPosOf512b = make(map[int64]struct{})
	flipActiveBitsEveryN(tree, 3011)
	nList = tree.syncMT4ActiveBits()
	assert.Equal(t, nList1, nList)
	fmt.Printf("here2 %#v\n", nList)
	checkAllTwigs(tree)

	tree.touchedPosOf512b = make(map[int64]struct{})
	flipActiveBitsEveryN(tree, 5000)
	nList = tree.syncMT4ActiveBits()
	assert.Equal(t, nList1, nList)
	fmt.Printf("here3 %#v\n", nList)
	checkAllTwigs(tree)

	tree.touchedPosOf512b = make(map[int64]struct{})
	flipActiveBitsEveryN(tree, 9001)
	nList = tree.syncMT4ActiveBits()
	assert.Equal(t, nList2, nList)
	fmt.Printf("here4 %#v\n", nList)
	checkAllTwigs(tree)
}

func maxNPlus1AtLevel(youngestTwigID int64, level int) int64 {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	shift := level - FirstLevelAboveTwig
	maxN := youngestTwigID >> shift
	mask := int64((1<<shift)-1)
	if (youngestTwigID & mask) != 0 {
		maxN += 1
	}
	return maxN
}

// make sure that nodes in range [start, end] exist at FirstLevelAboveTwig,
// and their upper-level nodes also exist
func checkNodeExistence(tree *Tree, start, end int64) {
	maxLevel := calcMaxLevel(end)
	for level := FirstLevelAboveTwig; level <= maxLevel; level++ {
		s := maxNAtLevel(start, level)
		e := maxNPlus1AtLevel(end, level)
		//fmt.Printf("Fuck level %d s %d e %d\n", level, s, e)
		for i := s; i < e; i++ {
			nodePos := Pos(level, i)
			if _, ok := tree.nodes[nodePos]; !ok {
				fmt.Printf("Why? we cannot find node at %d-%d\n", level, i)
				panic(fmt.Sprintf("Why? we cannot find node at %d-%d\n", level, i))
			}
		}
	}
}

// create n twigs and sync up their upper nodes
func initTwigsAndUpperNodes(tree *Tree, n int64) {
	tree.activeTwigs = make(map[int64]*Twig)
	nList := make([]int64, 0, int(n))
	for i := int64(0); i < n; i++ {
		//fmt.Printf("Now create twig %d\n", i)
		tree.activeTwigs[i] = CopyNullTwig()
		for j := 0; j < 32; j+=2 {
			binary.LittleEndian.PutUint16(tree.activeTwigs[i].twigRoot[j:j+2], uint16(i))
		}
		if len(nList) == 0 || nList[len(nList)-1] != i/2 {
			nList = append(nList, i/2)
		}
	}
	tree.youngestTwigID = n - 1
	tree.nodes = make(map[NodePos]*[32]byte, 2*n)
	tree.syncUpperNodes(nList)
}

// change the roots of twigs specified in idList to different values
func changeTwigRoots(tree *Tree, idList []int64) []int64 {
	nList := make([]int64, 0, len(idList))
	for _, twigID := range idList {
		twig := tree.activeTwigs[twigID]
		//fmt.Printf("Now change twig %d %v\n", twigID, ok)
		for i := range twig.twigRoot[:] {
			twig.twigRoot[i]++
		}
		if len(nList) == 0 || nList[len(nList)-1] != twigID/2 {
			nList = append(nList, twigID/2)
		}
	}
	return nList
}


func TestTreeSyncUpperNodes(t *testing.T) {
	tree := &Tree{}
	initTwigsAndUpperNodes(tree, 171)
	checkNodeExistence(tree, 0, 85)
	fmt.Printf("checkNodeExistence finished\n")
	checkUpperNodes(tree)
	for twigID := int64(0); twigID <= 80; twigID++ {
		// delete the twig and store its twigRoot in nodes
		pos := Pos(FirstLevelAboveTwig-1, twigID)
		twig := tree.activeTwigs[twigID]
		tree.nodes[pos] = &twig.twigRoot
		delete(tree.activeTwigs, twigID)
	}
	bz := tree.ReapNodes(0, 80)
	edgeNodes := BytesToEdgeNodes(bz)
	for _, en := range edgeNodes {
		n := int64(en.Pos)
		fmt.Printf("edge node: %d-%d\n", n>>56, (n<<8)>>8)
	}
	checkUpperNodes(tree)
	nList := changeTwigRoots(tree, []int64{81, 82, 99, 122, 123, 133, 139, 155, 166, 169, 170})
	tree.syncUpperNodes(nList)
}

const defaultFileSize = 8*4096*32

// build a tree for test: append countBefore entries before applying deactSNList,
// and append countAfter entries after applying deactSNList
func buildTestTree(dirName string, deactSNList []int64, countBefore, countAfter int) (*Tree, []int64, int64) {
	tree := NewEmptyTree(SmallBufferSize, defaultFileSize, dirName)
	entry := &Entry{
		Key:        []byte("key"),
		Value:      []byte("value"),
		NextKey:    []byte("nextkey"),
		Height:     100,
		LastHeight: 99,
		SerialNum:  0,
	}
	posList := make([]int64, 0, LeafCountInTwig+10)
	posList = append(posList, tree.AppendEntry(entry))

	for i := 1; i < countBefore; i++ {
		entry.SerialNum = int64(i)
		posList = append(posList, tree.AppendEntry(entry))
	}
	for _, sn := range deactSNList {
		tree.DeactiviateEntry(sn)
	}

	entry.SerialNum = int64(countBefore)
	posList = append(posList, tree.AppendEntry(entry))

	for i := 0; i < countAfter-1; i++ {
		entry.SerialNum++
		posList = append(posList, tree.AppendEntry(entry))
	}

	return tree, posList, entry.SerialNum
}

func TestTreeAppendEntry(t *testing.T) {
	dirName := "./DataTree"
	os.RemoveAll(dirName)
	os.Mkdir(dirName, 0700)
	deactSNList := []int64{101, 999, 1002}
	tree, posList, maxSerialNum := buildTestTree(dirName, deactSNList, TwigMask, 6)
	tree.EndBlock()

	for i, pos := range posList {
		entry, snList, _ := tree.entryFile.ReadEntryAndSNList(pos)
		assert.Equal(t, int64(i), entry.SerialNum)
		if i == TwigMask {
			assert.Equal(t, deactSNList, snList)
		} else {
			assert.Equal(t, 0, len(snList))
		}
	}

	activeList := make([]int64, 0, maxSerialNum)
	for i := int64(0); i <= maxSerialNum; i++ {
		active := true
		for j := range deactSNList {
			if i == deactSNList[j] {
				active = false
			}
		}
		assert.Equal(t, active, tree.GetActiveBit(i))
		if active {
			activeList = append(activeList, i)
		}
	}

	CheckHashConsistency(tree)

	assert.Equal(t, false, tree.TwigCanBePruned(0))
	assert.Equal(t, false, tree.TwigCanBePruned(1))

	entryChan := tree.GetActiveEntriesInTwig(0)
	i := 0
	for entryBz := range entryChan {
		sn := int64(binary.LittleEndian.Uint64(entryBz[len(entryBz)-8:]))
		if activeList[i] != sn {
			fmt.Printf("entryBz %#v sn %x\n", entryBz, activeList[i])
		}
		assert.Equal(t, activeList[i], sn)
		i++
	}

	tree.Flush()
	tree.Close()
	os.RemoveAll(dirName)
}

