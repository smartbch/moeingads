package datatree

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"sort"

	"github.com/dterei/gotsc"
	sha256 "github.com/minio/sha256-simd"
)

//var Debug bool
const Debug = false

const (
	FirstLevelAboveTwig int = 13
	MinPruneCount int64 = 2
	entriesPath = "entries"
	twigMtPath = "twigmt"
	nodesPath = "nodes.dat"
	mtree4YTPath = "mtree4YT.dat"
	twigsPath = "twigs.dat"
	DeactivedSNListMaxLen = 64
)

/*
                 ____TwigRoot___                Level_12
                /               \
	       /                \
1       leftRoot                activeBitsMTL3   Level_11
2	Level_10	2	activeBitsMTL2
4	Level_9		4	activeBitsMTL1
8	Level_8    8*32bytes	activeBits
16	Level_7
32	Level_6
64	Level_5
128	Level_4
256	Level_3
512	Level_2
1024	Level_1
2048	Level_0
*/

const (
	TwigShift       = 11
	LeafCountInTwig = 1 << TwigShift // 2048
	TwigMask        = LeafCountInTwig - 1
)

var NullTwig Twig
var NullMT4Twig [4096][32]byte
var NullNodeInHigherTree [64][32]byte

func CopyNullTwig() *Twig {
	var twig Twig
	twig = NullTwig
	return &twig
}

type Twig struct {
	activeBits     [256]byte
	activeBitsMTL1 [4][32]byte
	activeBitsMTL2 [2][32]byte
	activeBitsMTL3 [32]byte
	leftRoot       [32]byte
	twigRoot       [32]byte
	FirstEntryPos  int64
}

var Phase1Time, Phase2Time, Phase3Time, tscOverhead uint64

func init() {
	tscOverhead = gotsc.TSCOverhead()
	NullTwig.FirstEntryPos = -1
	for i := 0; i < 256; i++ {
		NullTwig.activeBits[i] = 0
	}
	var h Hasher
	NullTwig.syncL1(0, &h)
	NullTwig.syncL1(1, &h)
	NullTwig.syncL1(2, &h)
	NullTwig.syncL1(3, &h)
	h.Run()
	NullTwig.syncL2(0, &h)
	NullTwig.syncL2(1, &h)
	h.Run()
	NullTwig.syncL3(&h)
	h.Run()

	nullEntry := NullEntry()
	bz := EntryToBytes(nullEntry, nil)
	nullHash := hash(bz)
	level := byte(0)
	for stripe := 2048; stripe >= 1; stripe = stripe >> 1 {
		// use nullHash to fill one level of nodes
		for i := 0; i < stripe; i++ {
			copy(NullMT4Twig[stripe+i][:], nullHash[:])
		}
		nullHash = hash2(level, nullHash, nullHash)
		level++
	}
	copy(NullTwig.leftRoot[:], NullMT4Twig[1][:])

	NullTwig.syncTop(&h)
	h.Run()

	node := hash2(byte(FirstLevelAboveTwig-1), NullTwig.twigRoot[:], NullTwig.twigRoot[:])
	copy(NullNodeInHigherTree[FirstLevelAboveTwig][:], node)
	for i := FirstLevelAboveTwig + 1; i < len(NullNodeInHigherTree); i++ {
		node = hash2(byte(i-1), NullNodeInHigherTree[i-1][:], NullNodeInHigherTree[i-1][:])
		copy(NullNodeInHigherTree[i][:], node)
	}
}

func (twig *Twig) syncL1(pos int, h *Hasher) {
	switch pos {
	case 0:
		h.Add(8, twig.activeBitsMTL1[0][:], twig.activeBits[32*0:32*1], twig.activeBits[32*1:32*2])
	case 1:
		h.Add(8, twig.activeBitsMTL1[1][:], twig.activeBits[32*2:32*3], twig.activeBits[32*3:32*4])
	case 2:
		h.Add(8, twig.activeBitsMTL1[2][:], twig.activeBits[32*4:32*5], twig.activeBits[32*5:32*6])
	case 3:
		h.Add(8, twig.activeBitsMTL1[3][:], twig.activeBits[32*6:32*7], twig.activeBits[32*7:32*8])
	default:
		panic("Can not reach here!")
	}
}

func (twig *Twig) syncL2(pos int, h *Hasher) {
	switch pos {
	case 0:
		h.Add(9, twig.activeBitsMTL2[0][:], twig.activeBitsMTL1[0][:], twig.activeBitsMTL1[1][:])
	case 1:
		h.Add(9, twig.activeBitsMTL2[1][:], twig.activeBitsMTL1[2][:], twig.activeBitsMTL1[3][:])
	default:
		panic("Can not reach here!")
	}
}

func (twig *Twig) syncL3(h *Hasher) {
	h.Add(10, twig.activeBitsMTL3[:], twig.activeBitsMTL2[0][:], twig.activeBitsMTL2[1][:])
}

func (twig *Twig) syncTop(h *Hasher) {
	h.Add(11, twig.twigRoot[:], twig.leftRoot[:], twig.activeBitsMTL3[:])
}

func (twig *Twig) setBit(offset int) {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	twig.activeBits[pos] |= mask
}
func (twig *Twig) clearBit(offset int) {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	twig.activeBits[pos] &= ^mask
}
func (twig *Twig) getBit(offset int) bool {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	return (twig.activeBits[pos] & mask) != 0
}

type NodePos int64

func Pos(level int, n int64) NodePos {
	return NodePos((int64(level) << 56) | n)
}

type EdgeNode struct {
	Pos   NodePos
	Value []byte
}

type Tree struct {
	entryFile  *EntryFile
	twigMtFile *TwigMtFile
	dirName    string

	// the nodes in high level tree (higher than twigs)
	// this variable can be recovered from saved edge nodes and activeTwigs
	nodes map[NodePos]*[32]byte

	// these three variables can be recovered from entry file
	youngestTwigID     int64
	activeTwigs        map[int64]*Twig
	mtree4YoungestTwig [4096][32]byte
	leave4YoungestTwig [2048][2][]byte

	// The following variables are only used during the execution of one block
	mtree4YTChangeStart int
	mtree4YTChangeEnd   int
	twigsToBeDeleted    []int64
	touchedPosOf512b    map[int64]struct{}
	deactivedSNList     []int64
}

func NewEmptyTree(bufferSize, blockSize int, dirName string) *Tree {
	dirEntry := filepath.Join(dirName, entriesPath)
	os.Mkdir(dirEntry, 0700)
	entryFile, err := NewEntryFile(bufferSize, blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath)
	os.Mkdir(dirTwigMt, 0700)
	twigMtFile, err := NewTwigMtFile(bufferSize, blockSize, dirTwigMt)
	if err != nil {
		panic(err)
	}
	tree := &Tree{
		entryFile:  &entryFile,
		twigMtFile: &twigMtFile,
		dirName:    dirName,

		nodes:          make(map[NodePos]*[32]byte),
		youngestTwigID: 0,
		activeTwigs:    make(map[int64]*Twig),

		mtree4YTChangeStart: -1,
		mtree4YTChangeEnd:   -1,
		twigsToBeDeleted:    make([]int64, 0, 10),
		touchedPosOf512b:    make(map[int64]struct{}),
		deactivedSNList:     make([]int64, 0, 10),
	}
	var zero [32]byte
	tree.nodes[Pos(FirstLevelAboveTwig, 0)] = &zero
	tree.mtree4YoungestTwig = NullMT4Twig
	tree.activeTwigs[0] = CopyNullTwig()
	return tree
}

func (tree *Tree) Close() {
	tree.entryFile.Close()
	tree.twigMtFile.Close()
	tree.entryFile = nil
	tree.twigMtFile = nil
	tree.nodes = nil
	tree.activeTwigs = nil
	tree.twigsToBeDeleted = nil
	tree.touchedPosOf512b = nil
	tree.deactivedSNList = nil
}

func calcMaxLevel(youngestTwigID int64) int {
	return FirstLevelAboveTwig + 63 - bits.LeadingZeros64(uint64(youngestTwigID))
}

func (tree *Tree) GetFileSizes() (int64, int64) {
	return tree.entryFile.Size(), tree.twigMtFile.Size()
}

func (tree *Tree) TruncateFiles(entryFileSize, twigMtFileSize int64) {
	tree.entryFile.Truncate(entryFileSize)
	tree.twigMtFile.Truncate(twigMtFileSize)
}

func (tree *Tree) ReadEntry(pos int64) (entry *Entry) {
	entry, _ = tree.entryFile.ReadEntry(pos)
	return
}

func (tree *Tree) GetActiveBit(sn int64) bool {
	twigID := sn >> TwigShift
	return tree.activeTwigs[twigID].getBit(int(sn & TwigMask))
}

func (tree *Tree) setEntryActiviation(sn int64, active bool) {
	twigID := sn >> TwigShift
	if active {
		tree.activeTwigs[twigID].setBit(int(sn & TwigMask))
	} else {
		tree.activeTwigs[twigID].clearBit(int(sn & TwigMask))
		tree.deactivedSNList = append(tree.deactivedSNList, sn)
	}
	tree.touchedPosOf512b[sn/512] = struct{}{}
}

func (tree *Tree) ActiviateEntry(sn int64) {
	tree.setEntryActiviation(sn, true)
}

func (tree *Tree) DeactiviateEntry(sn int64) int {
	//if sn >> TwigShift == 50 {
	//	fmt.Printf("DeactiviateEntry sn %d twig %d\n", sn, sn >> TwigShift)
	//}
	tree.setEntryActiviation(sn, false)
	return len(tree.deactivedSNList)
}

func (tree *Tree) AppendEntry(entry *Entry) int64 {
	// write the entry while flushing deactivedSNList
	bz := EntryToBytes(*entry, tree.deactivedSNList)
	tree.deactivedSNList = tree.deactivedSNList[:0] // clear its content
	return tree.appendEntry([2][]byte{bz, nil}, entry.SerialNum)
}

func (tree *Tree) AppendEntryRawBytes(entryBz []byte, sn int64) int64 {
	// write the entry while flushing deactivedSNList
	entryBz[0] = byte(len(tree.deactivedSNList)) // change 1b snlist length
	bz := SNListToBytes(tree.deactivedSNList)
	tree.deactivedSNList = tree.deactivedSNList[:0] // clear its content
	return tree.appendEntry([2][]byte{entryBz, bz}, sn)
}

func (tree *Tree) appendEntry(bzTwo [2][]byte, sn int64) int64 {
	//update youngestTwigID
	twigID := sn >> TwigShift
	tree.youngestTwigID = twigID
	// mark this entry as valid
	tree.ActiviateEntry(sn)
	// record ChangeStart/ChangeEnd for endblock sync
	position := int(sn & TwigMask)
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = position
	}
	tree.mtree4YTChangeEnd = position

	pos := tree.entryFile.Append(bzTwo)
	// update the corresponding leaf of merkle tree
	//copy(tree.mtree4YoungestTwig[LeafCountInTwig+position][:], hash(bz))
	tree.leave4YoungestTwig[position] = bzTwo

	//!! if bzTwo[1] != nil {
	//!! 	fmt.Printf("Fuck bzTwo pos %d %#v\n", pos, bzTwo)
	//!! }

	if position == 0 { // when this is the first entry of current twig
		//fmt.Printf("Here FirstEntryPos of %d : %d\n", twigID, pos)
		tree.activeTwigs[twigID].FirstEntryPos = pos
	} else if position == TwigMask { // when this is the last entry of current twig
		// write the merkle tree of youngest twig to twigMtFile
		tree.syncMT4YoungestTwig()
		twig := tree.activeTwigs[twigID]
		tree.twigMtFile.AppendTwig(tree.mtree4YoungestTwig[1:], twig.FirstEntryPos)
		// allocate new twig as youngest twig
		tree.youngestTwigID++
		tree.activeTwigs[tree.youngestTwigID] = CopyNullTwig()
		tree.mtree4YoungestTwig = NullMT4Twig
		tree.touchedPosOf512b[(sn+1)/512] = struct{}{}
	}
	return pos
}

//!! func (tree *Tree) GetActiveEntriesInTwigOld(twigID int64) chan *Entry {
//!! 	twig := tree.activeTwigs[twigID]
//!! 	return tree.entryFile.GetActiveEntriesInTwigOld(twig)
//!! }

func (tree *Tree) GetActiveEntriesInTwig(twigID int64) chan []byte {
	twig := tree.activeTwigs[twigID]
	return tree.entryFile.GetActiveEntriesInTwig(twig)
}

func (tree *Tree) TwigCanBePruned(twigID int64) bool {
	// Can not prune an active twig
	_, ok := tree.activeTwigs[twigID]
	return !ok
}

// Prune the twigs between startID and endID
func (tree *Tree) PruneTwigs(startID, endID int64) []byte {
	if endID - startID < MinPruneCount {
		panic(fmt.Sprintf("The count of pruned twigs is too small: %d", endID-startID))
	}
	tree.entryFile.PruneHead(tree.twigMtFile.GetFirstEntryPos(endID))
	tree.twigMtFile.PruneHead(endID * TwigMtSize)
	return tree.ReapNodes(startID, endID)
}

func (tree *Tree) ReapNodes(start, end int64) []byte {
	tree.removeUselessNodes(start, end)
	return EdgeNodesToBytes(tree.getEdgeNodes(end))
}

func (tree *Tree) removeUselessNodes(start, end int64) {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	for level := FirstLevelAboveTwig-1; level <= maxLevel; level++ {
		endRound := end
		if end%2 != 0 && level != FirstLevelAboveTwig-1 {
			endRound--
		}
		for i := start-1; i < endRound; i++ { // minus 1 from start to cover some margin nodes
			pos := Pos(level, i)
			delete(tree.nodes, pos)
		}
		start >>= 1
		end >>= 1
	}
}

func (tree *Tree) getEdgeNodes(end int64) (newEdgeNodes []*EdgeNode) {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	for level := FirstLevelAboveTwig-1; level <= maxLevel; level++ {
		endRound := end
		if end%2 != 0 && level != FirstLevelAboveTwig-1 {
			endRound--
		}
		pos := Pos(level, endRound)
		hash, ok := tree.nodes[pos]
		if ok {
			edgeNode := &EdgeNode{Pos: pos, Value: (*hash)[:]}
			newEdgeNodes = append(newEdgeNodes, edgeNode)
		} else {
			panic(fmt.Sprintf("What? can not find %d-%d\n", level, end))
		}
		end >>= 1
	}
	return newEdgeNodes
}

func (tree *Tree) EvictTwig(twigID int64) {
	tree.twigsToBeDeleted = append(tree.twigsToBeDeleted, twigID)
}

func (tree *Tree) EndBlock() (rootHash []byte) {
	//start := gotsc.BenchStart()
	// sync up the merkle tree
	rootHash = tree.syncMT()
	// run the pending twig-deletion jobs
	// they were not deleted earlier becuase syncMT needs their content
	for _, twigID := range tree.twigsToBeDeleted {
		// delete the twig and store its twigRoot in nodes
		pos := Pos(FirstLevelAboveTwig-1, twigID)
		twig := tree.activeTwigs[twigID]
		tree.nodes[pos] = &twig.twigRoot
		//leftRoot := tree.twigMtFile.GetHashNode(twigID, 1)
		//twigRoot := hash2(11, leftRoot[:], NullTwig.activeBitsMTL3[:])
		//fmt.Printf("Here delete activeTwig %d-%d twigRoot: %v\n", FirstLevelAboveTwig-1, twigID, twig.twigRoot)
		//fmt.Printf("calculated twigRoot: %v\n", twigRoot)
		delete(tree.activeTwigs, twigID)
	}
	tree.twigsToBeDeleted = tree.twigsToBeDeleted[:0] // clear its content
	//Phase1Time += gotsc.BenchEnd() - start - tscOverhead
	//start = gotsc.BenchStart()
	tree.entryFile.FlushAsync()
	tree.twigMtFile.FlushAsync()
	//Phase2Time += gotsc.BenchEnd() - start - tscOverhead
	return
}

// following functions are used for syncing up merkle tree
func (tree *Tree) syncMT() []byte {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	tree.syncMT4YoungestTwig()
	nList := tree.syncMT4ActiveBits()
	//if Debug {
	//	fmt.Printf("nList: %v\n", nList)
	//}
	tree.syncUpperNodes(nList)
	tree.touchedPosOf512b = make(map[int64]struct{}) // clear the list
	hash := tree.nodes[Pos(maxLevel, 0)]
	return append([]byte{}, (*hash)[:]...) // copy and return the merkle root
}

func (tree *Tree) syncUpperNodes(nList []int64) {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	for level := FirstLevelAboveTwig; level <= maxLevel; level++ {
		//if Debug {
		//	fmt.Printf("syncNodesByLevel: %d %v\n", level, nList)
		//}
		nList = tree.syncNodesByLevel(level, nList)
	}
}

func maxNAtLevel(youngestTwigID int64, level int) int64 {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	shift := level - FirstLevelAboveTwig
	maxN := youngestTwigID >> shift
	return maxN
}

func (tree *Tree) getTwigRoot(n int64) ([32]byte, bool) {
	twig, ok := tree.activeTwigs[n]
	if ok {
		return twig.twigRoot, true
	}
	pos := Pos(FirstLevelAboveTwig-1, n)
	node, ok := tree.nodes[pos]
	if ok {
		return *node, true
	}
	var zero [32]byte
	return zero, false
}

func (tree *Tree) syncNodesByLevel(level int, nList []int64) []int64 {
	maxN := maxNAtLevel(tree.youngestTwigID, level)
	newList := make([]int64, 0, len(nList))
	var h Hasher
	for _, i := range nList {
		nodePos := Pos(level, i)
		if _, ok := tree.nodes[nodePos]; !ok {
			//if Debug {fmt.Printf("Now create parent node %d-%d\n", level, i)}
			var zeroHash [32]byte
			tree.nodes[nodePos] = &zeroHash
		}
		if level == FirstLevelAboveTwig {
			left, ok := tree.getTwigRoot(int64(2*i))
			if !ok {
				panic(fmt.Sprintf("Cannot find left twig root %d", 2*i))
			}
			right, ok := tree.getTwigRoot(int64(2*i+1))
			if !ok {
				right = NullTwig.twigRoot
			}
			parentNode := tree.nodes[nodePos]
			h.Add(byte(level-1), (*parentNode)[:], left[:], right[:])
			//if Debug {fmt.Printf("left: %#v right: %#v\n", left, right)}
			//if Debug {fmt.Printf("New Job: %d-%d %d- %d %d\n", level, i, level-1, 2*i, 2*i+1)}
		} else {
			nodePosL := Pos(level-1, 2*i)
			nodePosR := Pos(level-1, 2*i+1)
			if _, ok := tree.nodes[nodePosL]; !ok {
				panic(fmt.Sprintf("Failed to find the left child %d-%d %d- %d %d", level,i, level-1, 2*i, 2*i+1))
			}
			if _, ok := tree.nodes[nodePosR]; !ok {
				var h [32]byte
				copy(h[:], NullNodeInHigherTree[level][:])
				//if Debug {fmt.Printf("Here we create a node %d-%d\n", level-1, 2*i+1)}
				tree.nodes[nodePosR] = &h
				if 2*i != maxN && 2*i+1 != maxN {
					s := fmt.Sprintf("Not at the right edge, bug here. %d vs %d", 2*i, maxN)
					fmt.Println(s)
					panic(s)
				}
			}
			parentNode := tree.nodes[nodePos]
			nodeL := tree.nodes[nodePosL]
			nodeR := tree.nodes[nodePosR]
			h.Add(byte(level-1), (*parentNode)[:], (*nodeL)[:], (*nodeR)[:])
			//if Debug {fmt.Printf("left: %#v right: %#v\n", (*nodeL)[:], (*nodeR)[:])}
			//if Debug {fmt.Printf("New Job: %d-%d %d- %d %d\n", level, i, level-1, 2*i, 2*i+1)}
		}
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}
	h.Run()
	return newList
}

func (tree *Tree) syncMT4ActiveBits() []int64 {
	nList := make([]int64, 0, len(tree.touchedPosOf512b))
	for i := range tree.touchedPosOf512b {
		nList = append(nList, i)
	}
	sort.Slice(nList, func(i, j int) bool {return nList[i] < nList[j]})
	//if Debug {
	//	fmt.Printf("nList from touchedPosOf512b: %#v\n", nList)
	//}

	newList := make([]int64, 0, len(nList))
	var h Hasher
	for _, i := range nList {
		twigID := int64(i >> 2)
		tree.activeTwigs[twigID].syncL1(int(i&3), &h)
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}
	h.Run()
	nList = newList
	//if Debug {
	//	fmt.Printf("nList after syncL1: %#v\n", nList)
	//}
	newList = make([]int64, 0, len(nList))
	for _, i := range nList {
		twigID := int64(i >> 1)
		tree.activeTwigs[twigID].syncL2(int(i&1), &h)
		if len(newList) == 0 || newList[len(newList)-1] != twigID {
			newList = append(newList, twigID)
		}
	}
	h.Run()
	nList = newList
	//if Debug {
	//	fmt.Printf("nList after syncL2: %#v\n", nList)
	//}
	newList = make([]int64, 0, len(nList))
	for _, twigID := range nList {
		tree.activeTwigs[twigID].syncL3(&h)
	}
	h.Run()
	for _, twigID := range nList {
		tree.activeTwigs[twigID].syncTop(&h)
		if len(newList) == 0 || newList[len(newList)-1] != twigID/2 {
			newList = append(newList, twigID/2)
		}
	}
	h.Run()
	//if Debug {
	//	fmt.Printf("nList after syncTop: %#v\n", newList)
	//}
	return newList
}

/*         1
     2            3
   4   5       6     7
 8  9 a b    c   d  e  f
*/
// Sync up the merkle tree, between ChangeStart and ChangeEnd
func (tree *Tree) syncMT4YoungestTwig() {
	if tree.mtree4YTChangeStart == -1 {// nothing changed
		return
	}
	//for i := 0; i < LeafCountInTwig; i++ {
	//	if tree.leave4YoungestTwig[i] != nil {
	//		copy(tree.mtree4YoungestTwig[LeafCountInTwig+i][:], hash(tree.leave4YoungestTwig[i]))
	//		tree.leave4YoungestTwig[i] = nil
	//	}
	//}
	//for myIdx := tree.mtree4YTChangeStart; myIdx <= tree.mtree4YTChangeEnd; myIdx++ {
	//	copy(tree.mtree4YoungestTwig[myIdx][:], hash(tree.leave4YoungestTwig[myIdx]))
	//	tree.leave4YoungestTwig[myIdx] = nil
	//}
	sharedIdx := int64(-1)
	ParrallelRun(runtime.NumCPU(), func(workerID int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(tree.leave4YoungestTwig)) {break}
			if tree.leave4YoungestTwig[myIdx][0] == nil {continue}
			h := sha256.New()
			h.Write(tree.leave4YoungestTwig[myIdx][0])
			h.Write(tree.leave4YoungestTwig[myIdx][1])
			copy(tree.mtree4YoungestTwig[LeafCountInTwig+myIdx][:], h.Sum(nil))
			tree.leave4YoungestTwig[myIdx][0] = nil
		}
	})
	var h Hasher
	level := byte(0)
	start, end := tree.mtree4YTChangeStart, tree.mtree4YTChangeEnd
	for base := LeafCountInTwig; base >= 2; base >>= 1 {
		//fmt.Printf("base: %d\n", base)
		endRound := end
		if end%2 == 1 {
			endRound++
		}
		for j := (start &^ 1); j <= endRound && j+1 < base; j += 2 {
			i := base + j
			h.Add(level, tree.mtree4YoungestTwig[i/2][:], tree.mtree4YoungestTwig[i][:], tree.mtree4YoungestTwig[i+1][:])
			//fmt.Printf("Now job: %d-%d(%d) %d(%d) %d(%d)\n", level, j/2, i/2, j, i, j+1, i+1)
		}
		h.Run()
		start >>= 1
		end >>= 1
		level++
	}
	tree.mtree4YTChangeStart = -1 // reset its value
	tree.mtree4YTChangeEnd = 0
	copy(tree.activeTwigs[tree.youngestTwigID].leftRoot[:], tree.mtree4YoungestTwig[1][:])
}

