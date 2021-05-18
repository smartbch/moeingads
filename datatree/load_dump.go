package datatree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/mmcloughlin/meow"

	"github.com/smartbch/moeingads/types"
)

func LoadTwigFromFile(infile io.Reader) (twigID int64, twig Twig, err error) {
	var buf0, buf1 [8]byte
	var buf2 [4]byte
	var slices [13][]byte
	slices[0] = buf0[:]
	slices[1] = buf1[:]
	slices[2] = twig.activeBits[:]
	slices[3] = twig.activeBitsMTL1[0][:]
	slices[4] = twig.activeBitsMTL1[1][:]
	slices[5] = twig.activeBitsMTL1[2][:]
	slices[6] = twig.activeBitsMTL1[3][:]
	slices[7] = twig.activeBitsMTL2[0][:]
	slices[8] = twig.activeBitsMTL2[1][:]
	slices[9] = twig.activeBitsMTL3[:]
	slices[10] = twig.leftRoot[:]
	slices[11] = twig.twigRoot[:]
	slices[12] = buf2[:]
	h := meow.New32(0)
	for i, slice := range slices {
		_, err = infile.Read(slice)
		if err != nil {
			return
		}
		if i < 12 {
			_, _ = h.Write(slice)
		}
	}
	if !bytes.Equal(buf2[:], h.Sum(nil)) {
		err = errors.New("Checksum mismatch")
	}

	twigID = int64(binary.LittleEndian.Uint64(buf0[:]))
	twig.FirstEntryPos = int64(binary.LittleEndian.Uint64(buf1[:]))
	return
}

func (twig *Twig) Dump(twigID int64, outfile io.Writer) error {
	var buf0, buf1 [8]byte
	binary.LittleEndian.PutUint64(buf0[:], uint64(twigID))
	binary.LittleEndian.PutUint64(buf1[:], uint64(twig.FirstEntryPos))
	var slices [13][]byte
	slices[0] = buf0[:]
	slices[1] = buf1[:]
	slices[2] = twig.activeBits[:]
	slices[3] = twig.activeBitsMTL1[0][:]
	slices[4] = twig.activeBitsMTL1[1][:]
	slices[5] = twig.activeBitsMTL1[2][:]
	slices[6] = twig.activeBitsMTL1[3][:]
	slices[7] = twig.activeBitsMTL2[0][:]
	slices[8] = twig.activeBitsMTL2[1][:]
	slices[9] = twig.activeBitsMTL3[:]
	slices[10] = twig.leftRoot[:]
	slices[11] = twig.twigRoot[:]
	h := meow.New32(0)
	for _, slice := range slices[:12] {
		_, _ = h.Write(slice)
	}
	slices[12] = h.Sum(nil)
	for _, slice := range slices[:] {
		_, err := outfile.Write(slice)
		if err != nil {
			return err
		}
	}
	return nil
}

func EdgeNodesToBytes(edgeNodes []*EdgeNode) []byte {
	const stripe = 8 + 32
	res := make([]byte, len(edgeNodes)*stripe)
	for i, node := range edgeNodes {
		if len(node.Value) != 32 {
			s := fmt.Sprintf("node.Value %#v\n", node.Value)
			panic("len(node.Value) != 32 " + s)
		}
		binary.LittleEndian.PutUint64(res[i*stripe:i*stripe+8], uint64(node.Pos))
		copy(res[i*stripe+8:(i+1)*stripe], node.Value)
	}
	return res
}

func BytesToEdgeNodes(bz []byte) []*EdgeNode {
	const stripe = 8 + 32
	if len(bz)%stripe != 0 {
		panic("Invalid byteslice length for EdgeNodes")
	}
	res := make([]*EdgeNode, len(bz)/stripe)
	for i := 0; i < len(res); i++ {
		var value [32]byte
		pos := binary.LittleEndian.Uint64(bz[i*stripe : i*stripe+8])
		copy(value[:], bz[i*stripe+8:(i+1)*stripe])
		res[i] = &EdgeNode{Pos: NodePos(pos), Value: value[:]}
	}
	return res
}

func (tree *Tree) DumpNodes(outfile io.Writer) error {
	for pos, hash := range tree.nodes {
		edgeNode := &EdgeNode{Pos: pos, Value: (*hash)[:]}
		h := meow.New32(0)
		var bzList [2][]byte
		bzList[0] = EdgeNodesToBytes([]*EdgeNode{edgeNode})
		_, _ = h.Write(bzList[0])
		bzList[1] = h.Sum(nil)
		for _, bz := range bzList {
			_, err := outfile.Write(bz)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tree *Tree) LoadNodes(infile io.Reader) error {
	var buf [8 + 32 + 4]byte
	for {
		_, err := infile.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		h := meow.New32(0)
		_, _ = h.Write(buf[:8+32])
		if !bytes.Equal(buf[8+32:], h.Sum(nil)) {
			return errors.New("Checksum mismatch")
		}
		var hash [32]byte
		edgeNodes := BytesToEdgeNodes(buf[:8+32])
		copy(hash[:], edgeNodes[0].Value)
		tree.nodes[edgeNodes[0].Pos] = &hash
	}
	return nil
}

func (tree *Tree) DumpMtree4YT(outfile io.Writer) error {
	h := meow.New32(0)
	for _, buf := range tree.mtree4YoungestTwig[:] {
		_, _ = h.Write(buf[:])
		_, err := outfile.Write(buf[:])
		if err != nil {
			return err
		}
	}
	_, err := outfile.Write(h.Sum(nil))
	if err != nil {
		return err
	}
	return nil
}

func (tree *Tree) LoadMtree4YT(infile io.Reader) error {
	h := meow.New32(0)
	for i := range tree.mtree4YoungestTwig[:] {
		_, err := infile.Read(tree.mtree4YoungestTwig[i][:])
		if err != nil {
			return err
		}
		_, _ = h.Write(tree.mtree4YoungestTwig[i][:])
	}
	var buf [4]byte
	_, err := infile.Read(buf[:])
	if err != nil {
		return err
	}
	if !bytes.Equal(buf[:], h.Sum(nil)) {
		return errors.New("Checksum mismatch")
	}
	return nil
}

func (tree *Tree) Flush() {
	tree.entryFile.Flush()
	tree.twigMtFile.Flush()

	twigList := make([]int64, 0, len(tree.activeTwigs))
	for twigID := range tree.activeTwigs {
		twigList = append(twigList, twigID)
	}
	sort.Slice(twigList, func(i, j int) bool { return twigList[i] < twigList[j] })
	twigFile, err := os.OpenFile(filepath.Join(tree.dirName, twigsPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer twigFile.Close()
	for _, twigID := range twigList {
		_ = tree.activeTwigs[twigID].Dump(twigID, twigFile)
	}

	nodesFile, err := os.OpenFile(filepath.Join(tree.dirName, nodesPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer nodesFile.Close()
	err = tree.DumpNodes(nodesFile)
	if err != nil {
		panic(err)
	}

	mt4ytFile, err := os.OpenFile(filepath.Join(tree.dirName, mtree4YTPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer mt4ytFile.Close()
	err = tree.DumpMtree4YT(mt4ytFile)
	if err != nil {
		panic(err)
	}
}

func LoadTree(bufferSize, blockSize int, dirName string) *Tree {
	dirEntry := filepath.Join(dirName, entriesPath)
	entryFile, err := NewEntryFile(bufferSize, blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath)
	twigMtFile, err := NewTwigMtFile(bufferSize, blockSize, dirTwigMt)
	if err != nil {
		panic(err)
	}
	tree := &Tree{
		entryFile:  &entryFile,
		twigMtFile: &twigMtFile,
		dirName:    dirName,

		nodes:       make(map[NodePos]*[32]byte),
		activeTwigs: make(map[int64]*Twig),

		mtree4YTChangeStart: -1,
		mtree4YTChangeEnd:   -1,
		twigsToBeDeleted:    make([]int64, 0, 10),
		touchedPosOf512b:    make(map[int64]struct{}),
		deactivedSNList:     make([]int64, 0, 10),
	}

	twigFile, err := os.Open(filepath.Join(tree.dirName, twigsPath))
	if err != nil {
		panic(err)
	}
	defer twigFile.Close()
	for {
		twigID, twig, err := LoadTwigFromFile(twigFile)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		//fmt.Printf("Twig %d is loaded\n", twigID)
		tree.activeTwigs[twigID] = &twig
		if tree.youngestTwigID < twigID {
			tree.youngestTwigID = twigID
		}
	}

	nodesFile, err := os.Open(filepath.Join(tree.dirName, nodesPath))
	if err != nil {
		panic(err)
	}
	defer nodesFile.Close()
	err = tree.LoadNodes(nodesFile)
	if err != nil {
		panic(err)
	}

	mt4ytFile, err := os.Open(filepath.Join(tree.dirName, mtree4YTPath))
	if err != nil {
		panic(err)
	}
	defer mt4ytFile.Close()
	err = tree.LoadMtree4YT(mt4ytFile)
	if err != nil {
		panic(err)
	}
	return tree
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
	tree.ActiviateEntry(entry.SerialNum)
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
	for pos < size {
		//!! if pos > 108995312 {
		//!! 	entryBz, nxt := tree.entryFile.ReadEntryRawBytes(pos)
		//!! 	fmt.Printf("Fuck now pos %d %#v len=%d nxt=%d\n", pos, entryBz, len(entryBz), nxt)
		//!! }
		key, deactivedSNList, nextPos := tree.entryFile.ReadEntryAndSNList(pos)
		outChan <- types.EntryX{Entry: key, Pos: pos, DeactivedSNList: deactivedSNList}
		pos = nextPos
	}
	close(outChan)
}

func (tree *Tree) ScanEntriesLite(oldestActiveTwigID int64, outChan chan types.KeyAndPos) {
	pos := tree.twigMtFile.GetFirstEntryPos(oldestActiveTwigID)
	size := tree.entryFile.Size()
	for pos < size {
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
		//fmt.Printf("EdgeNode %d-%d\n", int64(edgeNode.Pos)>>56, (int64(edgeNode.Pos)<<8)>>8)
	}
	//fmt.Printf("syncUpperNodes %v\n", nList)
	//if len(nList) > 0 && nList[0] >= 2438 {Debug = true}
	return tree.syncUpperNodes(nList)
	//Debug = false
}

func (tree *Tree) RecoverInactiveTwigRoots(lastPrunedTwigID, oldestActiveTwigID int64) (newList []int64) {
	if lastPrunedTwigID < 0 {
		return
	}
	//fmt.Printf("RecoverInactiveTwigRoots lastPrunedTwigID %d oldestActiveTwigID %d\n", lastPrunedTwigID, oldestActiveTwigID)
	newList = make([]int64, 0, 1+(oldestActiveTwigID-lastPrunedTwigID)/2)
	for twigID := lastPrunedTwigID; twigID < oldestActiveTwigID; twigID++ {
		var twigRoot [32]byte
		leftRoot := tree.twigMtFile.GetHashNode(twigID, 1)
		copy(twigRoot[:], hash2(11, leftRoot[:], NullTwig.activeBitsMTL3[:]))
		pos := Pos(FirstLevelAboveTwig-1, twigID)
		tree.nodes[pos] = &twigRoot
		if len(newList) == 0 || newList[len(newList)-1] != twigID/2 {
			newList = append(newList, twigID/2)
		}
	}
	return
}

func RecoverTree(bufferSize, blockSize int, dirName string, edgeNodes []*EdgeNode, lastPrunedTwigID, oldestActiveTwigID, youngestTwigID int64, fileSizes []int64) (tree *Tree, rootHash [32]byte) {
	dirEntry := filepath.Join(dirName, entriesPath)
	entryFile, err := NewEntryFile(bufferSize, blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath)
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
	if startingInactiveTwigID%2 == 1 {
		startingInactiveTwigID--
	}
	nList0 := tree.RecoverInactiveTwigRoots(startingInactiveTwigID, oldestActiveTwigID)
	//fmt.Printf("Here lastPrunedTwigID %d oldestActiveTwigID %d nList0:%v\n", lastPrunedTwigID, oldestActiveTwigID, nList0)
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

func CompareTreeTwigs(treeA, treeB *Tree) {
	for twigID, a := range treeA.activeTwigs {
		b := treeB.activeTwigs[twigID]
		//!! fmt.Printf("twigID %d of %d\n", twigID, len(treeA.activeTwigs))
		CompareTwig(twigID, a, b)
	}
}

func CompareTreeNodes(treeA, treeB *Tree) {
	if len(treeA.nodes) != len(treeB.nodes) {
		panic("Different nodes count")
	}
	allSame := true
	for pos, hashA := range treeA.nodes {
		hashB := treeB.nodes[pos]
		if !bytes.Equal(hashA[:], hashB[:]) {
			fmt.Printf("Different Hash %d-%d", int64(pos)>>56, (int64(pos)<<8)>>8)
			allSame = false
		}
	}
	if !allSame {
		panic("Nodes Differ")
	}
}

func CompareTwig(twigID int64, a, b *Twig) {
	if !bytes.Equal(a.activeBits[:], b.activeBits[:]) {
		panic(fmt.Sprintf("activeBits differ at twig %d %#v %#v", twigID, a.activeBits[:], b.activeBits[:]))
	}
	for i := 0; i < len(a.activeBitsMTL1); i++ {
		if !bytes.Equal(a.activeBitsMTL1[i][:], b.activeBitsMTL1[i][:]) {
			panic(fmt.Sprintf("activeBitsMTL1[%d] differ at twig %d", i, twigID))
		}
	}
	for i := 0; i < len(a.activeBitsMTL2); i++ {
		if !bytes.Equal(a.activeBitsMTL2[i][:], b.activeBitsMTL2[i][:]) {
			panic(fmt.Sprintf("activeBitsMTL2[%d] differ at twig %d", i, twigID))
		}
	}
	if !bytes.Equal(a.activeBitsMTL3[:], b.activeBitsMTL3[:]) {
		panic(fmt.Sprintf("activeBitsMTL3 differ at twig %d", twigID))
	}
	if !bytes.Equal(a.leftRoot[:], b.leftRoot[:]) {
		panic(fmt.Sprintf("leftRoot differ at twig %d", twigID))
	}
	if !bytes.Equal(a.twigRoot[:], b.twigRoot[:]) {
		panic(fmt.Sprintf("twigRoot differ at twig %d", twigID))
	}
	if a.FirstEntryPos != b.FirstEntryPos {
		panic(fmt.Sprintf("FirstEntryPos differ at twig %d", twigID))
	}
}
