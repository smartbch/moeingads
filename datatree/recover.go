package datatree

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/smartbch/moeingads/types"
)

func EdgeNodesToBytes(edgeNodes []*EdgeNode) []byte {
	const stride = 8 + 32
	res := make([]byte, len(edgeNodes)*stride)
	for i, node := range edgeNodes {
		if len(node.Value) != 32 {
			s := fmt.Sprintf("node.Value %#v\n", node.Value)
			panic("len(node.Value) != 32 " + s)
		}
		binary.LittleEndian.PutUint64(res[i*stride:i*stride+8], uint64(node.Pos))
		copy(res[i*stride+8:(i+1)*stride], node.Value)
	}
	return res
}

func BytesToEdgeNodes(bz []byte) []*EdgeNode {
	const stride = 8 + 32
	if len(bz)%stride != 0 {
		panic("Invalid byteslice length for EdgeNodes")
	}
	res := make([]*EdgeNode, len(bz)/stride)
	for i := 0; i < len(res); i++ {
		var value [32]byte
		pos := binary.LittleEndian.Uint64(bz[i*stride : i*stride+8])
		copy(value[:], bz[i*stride+8:(i+1)*stride])
		res[i] = &EdgeNode{Pos: NodePos(pos), Value: value[:]}
	}
	return res
}

func (tree *Tree) RecoverEntry(pos int64, entry *Entry, deactivedSNList []int64, oldestActiveTwigID int64) {
	// deactive some old entry
	for _, sn := range deactivedSNList {
		twigID := sn >> TwigShift
		if twigID >= oldestActiveTwigID {
			tree.activeTwigs[sn>>TwigShift].clearBit(int(sn & TwigMask))
		}
	}
	//update youngestTwigID
	twigID := entry.SerialNum >> TwigShift
	tree.youngestTwigID = twigID
	// mark this entry as valid
	if string(entry.Key) == "dummy" {
		tree.touchedPosOf512b[entry.SerialNum/512] = struct{}{}
	} else {
		tree.ActiviateEntry(entry.SerialNum)
	}
	// record ChangeStart/ChangeEnd for endblock sync
	position := int(entry.SerialNum & TwigMask)
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = position
	}
	tree.mtree4YTChangeEnd = position

	// update the corresponding leaf of merkle tree
	bz := EntryToBytes(*entry, deactivedSNList)
	idx := entry.SerialNum & TwigMask
	copy(tree.mtree4YoungestTwig[LeafCountInTwig+idx][:], hash(bz))

	if idx == 0 { // when this is the first entry of current twig
		tree.activeTwigs[twigID].FirstEntryPos = pos
	} else if idx == TwigMask { // when this is the last entry of current twig
		// write the merkle tree of youngest twig to twigMtFile
		tree.syncMT4YoungestTwig()
		// allocate new twig as youngest twig
		tree.youngestTwigID++
		tree.activeTwigs[tree.youngestTwigID] = CopyNullTwig()
		tree.mtree4YoungestTwig = NullMT4Twig
	}
}

func (tree *Tree) ScanEntries(oldestActiveTwigID int64, outChan chan types.EntryX) {
	pos := tree.twigMtFile.GetFirstEntryPos(oldestActiveTwigID)
	size := tree.entryFile.Size()
	total := size - pos
	step := total / 20
	startPos := pos
	lastPos := pos
	for pos < size {
		//!! if pos > 108995312 {
		//!! 	entryBz, nxt := tree.entryFile.ReadEntryRawBytes(pos)
		//!! 	fmt.Printf("Fuck now pos %d %#v len=%d nxt=%d\n", pos, entryBz, len(entryBz), nxt)
		//!! }
		if (pos-startPos)/step != (lastPos-startPos)/step {
			fmt.Printf("ScanEntries %1.2f %d/%d\n", float64(pos-startPos)/float64(total), pos-startPos, total)
		}
		lastPos = pos
		key, deactivedSNList, nextPos := tree.entryFile.ReadEntryAndSNList(pos)
		outChan <- types.EntryX{Entry: key, Pos: pos, DeactivedSNList: deactivedSNList}
		pos = nextPos
	}
	close(outChan)
}

func (tree *Tree) ScanEntriesLite(oldestActiveTwigID int64, outChan chan types.KeyAndPos) {
	pos := tree.twigMtFile.GetFirstEntryPos(oldestActiveTwigID)
	size := tree.entryFile.Size()
	total := size - pos
	step := total / 20
	startPos := pos
	lastPos := pos
	for pos < size {
		if (pos-startPos)/step != (lastPos-startPos)/step {
			fmt.Printf("ScanEntriesLite %1.2f %d/%d\n", float64(pos-startPos)/float64(total), pos-startPos, total)
		}
		lastPos = pos
		entryBz, next := tree.entryFile.ReadEntryRawBytes(pos)
		outChan <- types.KeyAndPos{
			Key:       ExtractKeyFromRawBytes(entryBz),
			Pos:       pos,
			SerialNum: ExtractSerialNum(entryBz),
		}
		pos = next
	}
	close(outChan)
}

func (tree *Tree) RecoverActiveTwigs(oldestActiveTwigID int64) []int64 {
	entryXChan := make(chan types.EntryX, 100)
	go tree.ScanEntries(oldestActiveTwigID, entryXChan)
	for e := range entryXChan {
		tree.RecoverEntry(e.Pos, e.Entry, e.DeactivedSNList, oldestActiveTwigID)
	}
	//fmt.Printf("start %d end %d\n", tree.mtree4YTChangeStart, tree.mtree4YTChangeEnd)
	tree.syncMT4YoungestTwig()
	//if tree.youngestTwigID == 0x1ef {
	//	fmt.Printf("====== RecoverActiveTwigs after syncMT4YoungestTwig 0x1ef =====\n")
	//	for i, mtNode := range tree.mtree4YoungestTwig {
	//		fmt.Printf("MT %d %#v\n", i, mtNode)
	//	}
	//}
	//fmt.Printf("leftRoot %#v\n", tree.activeTwigs[tree.youngestTwigID].leftRoot[:])
	//fmt.Printf("RecoverActiveTwigs touchedPosOf512b %v\n", tree.touchedPosOf512b)
	//idList := make([]int, 0, len(tree.activeTwigs))
	//for id := range tree.activeTwigs {
	//	idList = append(idList, int(id))
	//}
	//sort.Ints(idList)
	//fmt.Printf("RecoverActiveTwigs activeTwigs %v\n", idList)
	nList := tree.syncMT4ActiveBits()
	tree.touchedPosOf512b = make(map[int64]struct{}) // clear the list
	return nList
}

func (tree *Tree) RecoverUpperNodes(edgeNodes []*EdgeNode, nList []int64) [32]byte {
	for _, edgeNode := range edgeNodes {
		var buf [32]byte
		copy(buf[:], edgeNode.Value)
		tree.nodes[edgeNode.Pos] = &buf
		//fmt.Printf("EdgeNode %d-%d\n", edgeNode.Pos.Level(), edgeNode.Pos.Nth())
	}
	//fmt.Printf("syncUpperNodes %v\n", nList)
	//if len(nList) > 0 && nList[0] >= 2438 {Debug = true}
	return tree.syncUpperNodes(nList)
	//Debug = false
}

func (tree *Tree) RecoverInactiveTwigRoots(lastPrunedTwigID, oldestActiveTwigID int64) {
	//fmt.Printf("RecoverInactiveTwigRoots lastPrunedTwigID %d oldestActiveTwigID %d\n", lastPrunedTwigID, oldestActiveTwigID)
	for twigID := lastPrunedTwigID; twigID < oldestActiveTwigID; twigID++ {
		var twigRoot [32]byte
		leftRoot := tree.twigMtFile.GetHashNode(twigID, 1, nil)
		copy(twigRoot[:], hash2(11, leftRoot[:], NullTwig.activeBitsMTL3[:]))
		pos := Pos(FirstLevelAboveTwig-1, twigID)
		tree.nodes[pos] = &twigRoot
	}
}

func RecoverTree(bufferSize, blockSize int, dirName, suffix string, edgeNodes []*EdgeNode, lastPrunedTwigID, oldestActiveTwigID, youngestTwigID int64, fileSizes []int64) (tree *Tree, rootHash [32]byte) {
	dirEntry := filepath.Join(dirName, entriesPath+suffix)
	entryFile, err := NewEntryFile(bufferSize, blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath+suffix)
	twigMtFile, err := NewTwigMtFile(bufferSize, blockSize, dirTwigMt)
	if err != nil {
		panic(err)
	}
	tree = &Tree{
		entryFile:  &entryFile,
		twigMtFile: &twigMtFile,
		dirName:    dirName,

		nodes:          make(map[NodePos]*[32]byte),
		activeTwigs:    make(map[int64]*Twig),
		youngestTwigID: youngestTwigID,

		mtree4YTChangeStart: -1,
		mtree4YTChangeEnd:   -1,
		twigsToBeDeleted:    make([]int64, 0, 10),
		touchedPosOf512b:    make(map[int64]struct{}),
		deactivedSNList:     make([]int64, 0, 10),
	}
	if len(fileSizes) == 2 {
		fmt.Printf("OldSize entryFile %d twigMtFile %d\n", tree.entryFile.Size(), tree.twigMtFile.Size())
		fmt.Printf("NewSize entryFile %d twigMtFile %d\n", fileSizes[0], fileSizes[1])
		tree.TruncateFiles(fileSizes[0], fileSizes[1])
	}
	tree.activeTwigs[oldestActiveTwigID] = CopyNullTwig()
	tree.mtree4YoungestTwig = NullMT4Twig
	startingInactiveTwigID := lastPrunedTwigID
	if lastPrunedTwigID == -1 {
		startingInactiveTwigID = 0
	}
	if startingInactiveTwigID%2 == 1 {
		startingInactiveTwigID--
	}
	tree.RecoverInactiveTwigRoots(startingInactiveTwigID, oldestActiveTwigID)
	for _, edgeNode := range edgeNodes {
		if edgeNode.Pos.Level() == int64(FirstLevelAboveTwig-1) &&
			startingInactiveTwigID < edgeNode.Pos.Nth() {
			startingInactiveTwigID = edgeNode.Pos.Nth()
		}
	}
	nList0 := make([]int64, 0, 1+(oldestActiveTwigID-startingInactiveTwigID)/2)
	for twigID := startingInactiveTwigID; twigID < oldestActiveTwigID; twigID++ {
		if len(nList0) == 0 || nList0[len(nList0)-1] != twigID/2 {
			nList0 = append(nList0, twigID/2)
		}
	}
	//fmt.Printf("Here lastPrunedTwigID %d startingInactiveTwigID %d oldestActiveTwigID %d nList0:%v\n", lastPrunedTwigID, startingInactiveTwigID, oldestActiveTwigID, nList0)
	nList := tree.RecoverActiveTwigs(oldestActiveTwigID)
	//fmt.Printf("Here nList:%v\n", nList)
	var newList []int64
	if len(nList0) > 0 && len(nList) > 0 && nList0[len(nList0)-1] == nList[0] {
		newList = append(nList0, nList[1:]...)
	} else {
		newList = append(nList0, nList...)
	}
	rootHash = tree.RecoverUpperNodes(edgeNodes, newList)
	return
}
