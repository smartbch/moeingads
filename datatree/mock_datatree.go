package datatree

import (
	"fmt"

	"github.com/smartbch/moeingads/types"
)

type MockTwig struct {
	activeBits [LeafCountInTwig]bool
	entries    [LeafCountInTwig]Entry
}

type MockDataTree struct {
	twigs map[int64]*MockTwig
}

func NewMockDataTree() *MockDataTree {
	tree := &MockDataTree{
		twigs: make(map[int64]*MockTwig),
	}
	tree.twigs[0] = &MockTwig{}
	return tree
}

func (dt *MockDataTree) DeactiviateEntry(sn int64) int {
	//fmt.Printf("Here DataTree.DeactiviateEntry(sn=%d)\n", sn)
	twigID := sn >> TwigShift
	dt.twigs[twigID].activeBits[sn&TwigMask] = false
	return 0
}

func (dt *MockDataTree) AppendEntryRawBytes(entryBz []byte, sn int64) int64 {
	e := EntryFromRawBytes(entryBz)
	return dt.AppendEntry(e)
}

func (dt *MockDataTree) AppendEntry(entry *Entry) int64 {
	sn := entry.SerialNum
	twigID := sn >> TwigShift
	dt.twigs[twigID].entries[sn&TwigMask] = *entry
	dt.twigs[twigID].activeBits[sn&TwigMask] = true
	if (sn & TwigMask) == TwigMask {
		dt.twigs[twigID+1] = &MockTwig{}
	}
	return sn * 1024
}

func (dt *MockDataTree) ReadEntry(pos int64) *Entry {
	sn := pos / 1024
	twigID := sn >> TwigShift
	if !dt.twigs[twigID].activeBits[sn&TwigMask] {
		return nil
	}
	entry := dt.twigs[int64(twigID)].entries[sn&TwigMask]
	return &entry
}

func (dt *MockDataTree) GetActiveBit(sn int64) bool {
	twigID := sn >> TwigShift
	return dt.twigs[twigID].activeBits[sn&TwigMask]
}

func (dt *MockDataTree) EvictTwig(twigID int64) {
	delete(dt.twigs, twigID)
}

func (dt *MockDataTree) GetActiveEntriesInTwig(twigID int64) chan []byte {
	res := make(chan []byte, 100)
	go func() {
		twig := dt.twigs[twigID]
		for i, active := range twig.activeBits {
			if active {
				entry := twig.entries[i]
				res <- EntryToBytes(entry, nil)
			}
		}
	}()
	return res
}

func (dt *MockDataTree) ScanEntries(oldestActiveTwigID int64, outChan chan types.EntryX) {
	panic(fmt.Sprintf("ScanEntries not implemented. oldestActiveTwigID=%d", oldestActiveTwigID))
}

func (dt *MockDataTree) ScanEntriesLite(oldestActiveTwigID int64, outChan chan types.KeyAndPos) {
	panic(fmt.Sprintf("ScanEntriesLite not implemented. oldestActiveTwigID=%d", oldestActiveTwigID))
}

func (dt *MockDataTree) TwigCanBePruned(twigID int64) bool {
	_, ok := dt.twigs[twigID]
	return !ok
}

func (dt *MockDataTree) PruneTwigs(startID, endID int64) []byte {
	return nil
}

func (dt *MockDataTree) GetFileSizes() (int64, int64) {
	return 0, 0
}

func (dt *MockDataTree) EndBlock() (rootHash [32]byte) {
	return
}

func (dt *MockDataTree) Close() {
}

func (dt *MockDataTree) Flush() {
}
