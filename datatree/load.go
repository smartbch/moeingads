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
)

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

func (tree *Tree) SaveMemToDisk() {
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
			fmt.Printf("Different Hash %d-%d", pos.Level(), pos.Nth())
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

func (tree *Tree) PrintTree() {
	keys := make([]int64, 0, len(tree.activeTwigs))
	for key := range tree.activeTwigs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		twig := tree.activeTwigs[key]
		fmt.Printf("TWIG %x %#v %#v\n", key, twig.leftRoot, twig.activeBits)
	}

	keys = make([]int64, 0, len(tree.nodes))
	for key := range tree.nodes {
		keys = append(keys, int64(key))
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		fmt.Printf("NODE %x %#v\n", key, tree.nodes[NodePos(key)])
	}
}
