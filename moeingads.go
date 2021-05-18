package moeingads

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dterei/gotsc"

	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/indextree"
	"github.com/smartbch/moeingads/metadb"
	"github.com/smartbch/moeingads/types"
)

const (
	defaultFileSize                       = 1024 * 1024 * 1024
	StartReapThres                  int64 = 10000 // 1000 * 1000
	KeptEntriesToActiveEntriesRatio       = 2

	heMapSize = 128
	nkMapSize = 64
)

type MoeingADS struct {
	meta          types.MetaDB
	idxTree       types.IndexTree
	datTree       types.DataTree
	rocksdb       *indextree.RocksDB
	k2heMap       *BucketMap // key-to-hot-entry map
	k2nkMap       *BucketMap // key-to-next-key map
	tempEntries64 [64][]*HotEntry
	cachedEntries []*HotEntry
	startKey      []byte
	endKey        []byte
}

func NewMoeingADS4Mock(startEndKeys [][]byte) *MoeingADS {
	mads := &MoeingADS{
		k2heMap:  NewBucketMap(heMapSize),
		k2nkMap:  NewBucketMap(nkMapSize),
		startKey: append([]byte{}, startEndKeys[0]...),
		endKey:   append([]byte{}, startEndKeys[1]...),
	}

	mads.datTree = datatree.NewMockDataTree()
	mads.idxTree = indextree.NewMockIndexTree()

	var err error
	mads.rocksdb, err = indextree.NewRocksDB("rocksdb", "./")
	if err != nil {
		panic(err)
	}

	mads.meta = metadb.NewMetaDB(mads.rocksdb)
	mads.rocksdb.OpenNewBatch()
	mads.initGuards()
	return mads
}

func NewMoeingADS(dirName string, canQueryHistory bool, startEndKeys [][]byte) (*MoeingADS, error) {
	tscOverhead = gotsc.TSCOverhead()
	_, err := os.Stat(dirName)
	dirNotExists := os.IsNotExist(err)
	mads := &MoeingADS{
		k2heMap:       NewBucketMap(heMapSize),
		k2nkMap:       NewBucketMap(nkMapSize),
		cachedEntries: make([]*HotEntry, 0, 2000),
		startKey:      append([]byte{}, startEndKeys[0]...),
		endKey:        append([]byte{}, startEndKeys[1]...),
	}
	for i := range mads.tempEntries64 {
		mads.tempEntries64[i] = make([]*HotEntry, 0, len(mads.cachedEntries)/8)
	}
	if dirNotExists {
		_ = os.Mkdir(dirName, 0700)
	}

	mads.rocksdb, err = indextree.NewRocksDB("rocksdb", dirName)
	if err != nil {
		panic(err)
	}
	mads.meta = metadb.NewMetaDB(mads.rocksdb)
	if !dirNotExists {
		mads.meta.ReloadFromKVDB()
		//mads.meta.PrintInfo()
	}

	if dirNotExists { // Create a new database in this dir
		mads.datTree = datatree.NewEmptyTree(datatree.BufferSize, defaultFileSize, dirName)
		if canQueryHistory {
			mads.idxTree = indextree.NewNVTreeMem(mads.rocksdb)
		} else {
			mads.idxTree = indextree.NewNVTreeMem(nil)
		}
		mads.rocksdb.OpenNewBatch()
		mads.meta.Init()
		for i := 0; i < 2048; i++ {
			sn := mads.meta.GetMaxSerialNum()
			mads.meta.IncrMaxSerialNum()
			entry := datatree.DummyEntry(sn)
			mads.datTree.AppendEntry(entry)
		}
		mads.initGuards()
		mads.rocksdb.CloseOldBatch()
	} else if mads.meta.GetIsRunning() { // MoeingADS is *NOT* closed properly
		oldestActiveTwigID := mads.meta.GetOldestActiveTwigID()
		youngestTwigID := mads.meta.GetMaxSerialNum() >> datatree.TwigShift
		bz := mads.meta.GetEdgeNodes()
		edgeNodes := datatree.BytesToEdgeNodes(bz)
		//fmt.Printf("dirName %s oldestActiveTwigID: %d youngestTwigID: %d\n", dirName, oldestActiveTwigID, youngestTwigID)
		//fmt.Printf("edgeNodes %#v\n", edgeNodes)
		var recoveredRoot [32]byte
		mads.datTree, recoveredRoot = datatree.RecoverTree(datatree.BufferSize, defaultFileSize,
			dirName, edgeNodes, mads.meta.GetLastPrunedTwig(), oldestActiveTwigID, youngestTwigID,
			[]int64{mads.meta.GetEntryFileSize(), mads.meta.GetTwigMtFileSize()})
		//fmt.Println("Upper Tree At Recover"); mads.datTree.PrintTree()
		recordedRoot := mads.meta.GetRootHash()
		if recordedRoot != recoveredRoot {
			fmt.Printf("%#v %#v\n", recordedRoot, recoveredRoot)
			panic("Failed To Recover")
		}
	} else { // MoeingADS is closed properly
		mads.datTree = datatree.LoadTree(datatree.BufferSize, defaultFileSize, dirName)
	}

	if dirNotExists {
		//do nothing
	} else if canQueryHistory { // use rocksdb to keep the historical index
		mads.idxTree = indextree.NewNVTreeMem(mads.rocksdb)
		err = mads.idxTree.Init(nil)
		if err != nil {
			return nil, err
		}
	} else { // only latest index, no historical index at all
		mads.idxTree = indextree.NewNVTreeMem(nil)
		oldestActiveTwigID := mads.meta.GetOldestActiveTwigID()
		mads.idxTree.BeginWrite(0) // we set height=0 here, which will not be used
		keyAndPosChan := make(chan types.KeyAndPos, 100)
		go mads.datTree.ScanEntriesLite(oldestActiveTwigID, keyAndPosChan)
		for e := range keyAndPosChan {
			if string(e.Key) == "dummy" {
				continue
			}
			if mads.datTree.GetActiveBit(e.SerialNum) {
				mads.idxTree.Set(e.Key, e.Pos)
			}
		}
		mads.idxTree.EndWrite()
	}

	mads.meta.SetIsRunning(true)
	return mads, nil
}

func (mads *MoeingADS) PrintMetaInfo() {
	mads.meta.PrintInfo()
}

func (mads *MoeingADS) Close() {
	mads.meta.SetIsRunning(false)
	mads.idxTree.Close()
	mads.rocksdb.Close()
	mads.datTree.Flush()
	mads.datTree.Close()
	mads.meta.Close()
	mads.idxTree = nil
	mads.rocksdb = nil
	mads.datTree = nil
	mads.meta = nil
	mads.k2heMap = nil
	mads.k2nkMap = nil
}

type Entry = types.Entry
type HotEntry = types.HotEntry

func (mads *MoeingADS) GetRootHash() []byte {
	r := mads.meta.GetRootHash()
	return append([]byte{}, r[:]...)
}

func (mads *MoeingADS) GetEntry(k []byte) *Entry {
	pos, ok := mads.idxTree.Get(k)
	if !ok {
		return nil
	}
	e := mads.datTree.ReadEntry(int64(pos))
	if !mads.datTree.GetActiveBit(e.SerialNum) {
		panic(fmt.Sprintf("Reading inactive entry %d\n", e.SerialNum))
	}
	return e
}

func isFakeInserted(hotEntry *HotEntry) bool {
	return hotEntry.EntryPtr.SerialNum == -1 && hotEntry.EntryPtr.Value == nil
}

func isInserted(hotEntry *HotEntry) bool {
	return hotEntry.Operation == types.OpInsertOrChange && hotEntry.EntryPtr.SerialNum == -1
}

func isModified(hotEntry *HotEntry) bool {
	return hotEntry.Operation == types.OpInsertOrChange && hotEntry.EntryPtr.SerialNum >= 0
}

func (mads *MoeingADS) PrepareForUpdate(k []byte) {
	//fmt.Printf("In PrepareForUpdate we see: %s\n", string(k))
	pos, findIt := mads.idxTree.Get(k)
	if findIt { // The case of Change
		//fmt.Printf("In PrepareForUpdate we update\n")
		entry := mads.datTree.ReadEntry(int64(pos))
		//fmt.Printf("Now we add entry to k2e(findIt): %s(%#v)\n", string(k), k)
		mads.k2heMap.Store(string(k), &HotEntry{
			EntryPtr:  entry,
			Operation: types.OpNone,
		})
		return
	}
	prevEntry := mads.getPrevEntry(k)

	// The case of Insert
	//fmt.Printf("Now we add entry to k2e(not-findIt): %s(%#v)\n", string(k), k)
	mads.k2heMap.Store(string(k), &HotEntry{
		EntryPtr: &Entry{
			Key:        append([]byte{}, k...),
			Value:      nil,
			NextKey:    nil,
			Height:     0,
			LastHeight: 0,
			SerialNum:  -1, //inserted entries has negative SerialNum
		},
		Operation: types.OpNone,
	})

	//fmt.Printf("In PrepareForUpdate we insert\n")
	//fmt.Printf("prevEntry(%#v): %#v\n", k, prevEntry)
	//fmt.Printf("Now we add entry to k2e(prevEntry.Key): %s(%#v)\n", kStr, prevEntry.Key)
	mads.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone,
	})

	mads.k2nkMap.Store(string(prevEntry.NextKey), nil)
}

func (mads *MoeingADS) PrepareForDeletion(k []byte) (findIt bool) {
	//fmt.Printf("In PrepareForDeletion we see: %#v\n", k)
	pos, findIt := mads.idxTree.Get(k)
	if !findIt {
		return
	}

	entry := mads.datTree.ReadEntry(int64(pos))
	prevEntry := mads.getPrevEntry(k)

	//fmt.Printf("In PrepareForDeletion we read: %#v\n", entry)
	mads.k2heMap.Store(string(entry.Key), &HotEntry{
		EntryPtr:  entry,
		Operation: types.OpNone,
	})

	mads.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone,
	})

	mads.k2nkMap.Store(string(entry.NextKey), nil) // we do not need next entry's value, so here we store nil
	return
}

func isHintHotEntry(hotEntry *HotEntry) bool {
	return hotEntry.EntryPtr.SerialNum == math.MinInt64
}

func makeHintHotEntry(key string) *HotEntry {
	return &HotEntry{
		EntryPtr: &Entry{
			Key:        []byte(key),
			Value:      nil,
			NextKey:    nil,
			Height:     -1,
			LastHeight: -1,
			SerialNum:  math.MinInt64, // hint entry has smallest possible SerialNum
		},
		Operation: types.OpNone,
	}
}

func (mads *MoeingADS) getPrevEntry(k []byte) *Entry {
	iter := mads.idxTree.ReverseIterator(mads.startKey, k)
	defer iter.Close()
	if !iter.Valid() {
		panic(fmt.Sprintf("The iterator is invalid! Missing a guard node? k=%#v", k))
	}
	pos := iter.Value()
	//fmt.Printf("In getPrevEntry: %#v %d\n", iter.Key(), iter.Value())
	e := mads.datTree.ReadEntry(int64(pos))
	if !mads.datTree.GetActiveBit(e.SerialNum) {
		panic(fmt.Sprintf("Reading inactive entry %d\n", e.SerialNum))
	}
	return e
}

const (
	MinimumTasksInGoroutine = 10
	MaximumGoroutines       = 128
)

func (mads *MoeingADS) numOfKeptEntries() int64 {
	return mads.meta.GetMaxSerialNum() - mads.meta.GetOldestActiveTwigID()*datatree.LeafCountInTwig
}

func (mads *MoeingADS) GetCurrHeight() int64 {
	return mads.meta.GetCurrHeight()
}

func (mads *MoeingADS) BeginWrite(height int64) {
	mads.rocksdb.OpenNewBatch()
	mads.idxTree.BeginWrite(height)
	mads.meta.SetCurrHeight(height)
}

func (mads *MoeingADS) Set(key, value []byte) {
	hotEntry, ok := mads.k2heMap.Load(string(key))
	if !ok {
		panic("Can not find entry in cache")
	}
	if hotEntry == nil {
		panic("Can not change or insert at a fake entry")
	}
	//fmt.Printf("In Set we see: %#v %#v\n", key, value)
	hotEntry.EntryPtr.Value = value
	hotEntry.Operation = types.OpInsertOrChange
}

func (mads *MoeingADS) Delete(key []byte) {
	//fmt.Printf("In Delete we see: %s(%#v)\n", string(key), key)
	hotEntry, ok := mads.k2heMap.Load(string(key))
	if !ok {
		return // delete a non-exist kv pair
	}
	if hotEntry == nil {
		return // delete a non-exist kv pair
	}
	hotEntry.Operation = types.OpDelete
}

func getPrev(cachedEntries []*HotEntry, i int) int {
	var j int
	for j = i - 1; j >= 0; j-- {
		if cachedEntries[j].Operation != types.OpDelete && !isFakeInserted(cachedEntries[j]) {
			break
		}
	}
	if j < 0 {
		//for j = i; j >= 0; j-- {
		//	fmt.Printf("Debug j %d hotEntry %#v Entry %#v\n", j, cachedEntries[j], cachedEntries[j].EntryPtr)
		//}
		panic("Can not find previous entry")
	}
	return j
}

func getNext(cachedEntries []*HotEntry, i int) int {
	var j int
	for j = i + 1; j < len(cachedEntries); j++ {
		if cachedEntries[j].Operation != types.OpDelete && !isFakeInserted(cachedEntries[j]) {
			break
		}
	}
	if j >= len(cachedEntries) {
		//for j = i; j < len(cachedEntries); j++ {
		//	fmt.Printf("Debug j %d hotEntry %#v Entry %#v\n", j, cachedEntries[j], cachedEntries[j].EntryPtr)
		//}
		panic("Can not find next entry")
	}
	return j
}

func (mads *MoeingADS) update() {
	sharedIdx := int64(-1)
	datatree.ParrallelRun(runtime.NumCPU(), func(workerID int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= 64 {
				break
			}
			for _, e := range mads.k2heMap.maps[myIdx] {
				mads.tempEntries64[myIdx] = append(mads.tempEntries64[myIdx], e)
			}
			for k := range mads.k2nkMap.maps[myIdx] {
				if _, ok := mads.k2heMap.maps[myIdx][k]; ok {
					continue
				}
				mads.tempEntries64[myIdx] = append(mads.tempEntries64[myIdx], makeHintHotEntry(k))
			}
			entries := mads.tempEntries64[myIdx]
			sort.Slice(entries, func(i, j int) bool {
				return bytes.Compare(entries[i].EntryPtr.Key, entries[j].EntryPtr.Key) < 0
			})
		}
	})
	//@ start := gotsc.BenchStart()
	mads.cachedEntries = mads.cachedEntries[:0]
	for _, entries := range mads.tempEntries64 {
		mads.cachedEntries = append(mads.cachedEntries, entries...)
	}
	//@ Phase0Time += gotsc.BenchEnd() - start - tscOverhead
	// set NextKey to correct values and mark IsModified
	for i, hotEntry := range mads.cachedEntries {
		if hotEntry.Operation != types.OpNone && isHintHotEntry(hotEntry) {
			panic("Operate on a hint entry")
		}
		if isFakeInserted(hotEntry) {
			continue
		}
		if hotEntry.Operation == types.OpDelete {
			hotEntry.IsModified = true
			next := getNext(mads.cachedEntries, i)
			nextKey := mads.cachedEntries[next].EntryPtr.Key
			prev := getPrev(mads.cachedEntries, i)
			mads.cachedEntries[prev].EntryPtr.NextKey = nextKey
			mads.cachedEntries[prev].IsTouchedByNext = true
		} else if isInserted(hotEntry) {
			hotEntry.IsModified = true
			//fmt.Printf("THERE key: %#v HotEntry: %#v Entry: %#v\n", hotEntry.EntryPtr.Key, hotEntry, *(hotEntry.EntryPtr))
			next := getNext(mads.cachedEntries, i)
			hotEntry.EntryPtr.NextKey = mads.cachedEntries[next].EntryPtr.Key
			prev := getPrev(mads.cachedEntries, i)
			mads.cachedEntries[prev].EntryPtr.NextKey = hotEntry.EntryPtr.Key
			mads.cachedEntries[prev].IsTouchedByNext = true
			//fmt.Printf("this: %s(%#v) prev %d: %s(%#v) next %d: %s(%#v)\n", hotEntry.EntryPtr.Key, hotEntry.EntryPtr.Key,
			//	prev, mads.cachedEntries[prev].EntryPtr.Key, mads.cachedEntries[prev].EntryPtr.Key,
			//	next,  mads.cachedEntries[next].EntryPtr.Key, mads.cachedEntries[next].EntryPtr.Key)
		} else if isModified(hotEntry) {
			hotEntry.IsModified = true
		}
	}
	start := gotsc.BenchStart()
	// update stored data
	for _, hotEntry := range mads.cachedEntries {
		if !(hotEntry.IsModified || hotEntry.IsTouchedByNext) {
			continue
		}
		ptr := hotEntry.EntryPtr
		if hotEntry.Operation == types.OpDelete && ptr.SerialNum >= 0 {
			// if ptr.SerialNum==-1, then we are deleting a just-inserted value, so ignore it.
			//fmt.Printf("Now we deactive %d for deletion %#v\n", ptr.SerialNum, ptr)
			mads.idxTree.Delete(ptr.Key)
			mads.DeactiviateEntry(ptr.SerialNum)
		} else if hotEntry.Operation != types.OpNone || hotEntry.IsTouchedByNext {
			if ptr.SerialNum >= 0 { // if this entry already exists
				mads.DeactiviateEntry(ptr.SerialNum)
			}
			ptr.LastHeight = ptr.Height
			ptr.Height = mads.meta.GetCurrHeight()
			ptr.SerialNum = mads.meta.GetMaxSerialNum()
			//fmt.Printf("Now SerialNum = %d for %s(%#v) %#v Entry %#v\n", ptr.SerialNum, string(ptr.Key), ptr.Key, hotEntry, *ptr)
			mads.meta.IncrMaxSerialNum()
			//@ start := gotsc.BenchStart()
			pos := mads.datTree.AppendEntry(ptr)
			//@ Phase2Time += gotsc.BenchEnd() - start - tscOverhead
			mads.idxTree.Set(ptr.Key, pos)
		}
	}
	Phase1n2Time += gotsc.BenchEnd() - start - tscOverhead
}

func (mads *MoeingADS) DeactiviateEntry(sn int64) {
	pendingDeactCount := mads.datTree.DeactiviateEntry(sn)
	if pendingDeactCount > datatree.DeactivedSNListMaxLen {
		mads.flushDeactivedSNList()
	}
}

func (mads *MoeingADS) flushDeactivedSNList() {
	sn := mads.meta.GetMaxSerialNum()
	mads.meta.IncrMaxSerialNum()
	entry := datatree.DummyEntry(sn)
	mads.datTree.DeactiviateEntry(sn)
	mads.datTree.AppendEntry(entry)
}

func (mads *MoeingADS) CheckConsistency() {
	iter := mads.idxTree.ReverseIterator(mads.startKey, mads.endKey)
	defer iter.Close()
	nextKey := mads.endKey
	for iter.Valid() && !bytes.Equal(iter.Key(), mads.startKey) {
		pos := iter.Value()
		entry := mads.datTree.ReadEntry(int64(pos))
		if !bytes.Equal(entry.NextKey, nextKey) {
			panic(fmt.Sprintf("Invalid NextKey for %#v, datTree %#v, idxTree %#v\n",
				iter.Key(), entry.NextKey, nextKey))
		}
		nextKey = iter.Key()
		iter.Next()
	}
}

func (mads *MoeingADS) ActiveCount() int {
	return mads.idxTree.ActiveCount()
}

var Phase1n2Time, Phase1Time, Phase2Time, Phase3Time, Phase4Time, Phase0Time, tscOverhead uint64

func (mads *MoeingADS) EndWrite() {
	//fmt.Printf("EndWrite %d\n", mads.meta.GetCurrHeight())
	mads.update()
	//start := gotsc.BenchStart()
	//if mads.meta.GetActiveEntryCount() != int64(mads.idxTree.ActiveCount()) - 2 {
	//	panic(fmt.Sprintf("Fuck meta.GetActiveEntryCount %d mads.idxTree.ActiveCount %d\n", mads.meta.GetActiveEntryCount(), mads.idxTree.ActiveCount()))
	//}
	//fmt.Printf("begin numOfKeptEntries %d ActiveCount %d x2 %d\n", mads.numOfKeptEntries(), mads.idxTree.ActiveCount(), mads.idxTree.ActiveCount()*2)
	for mads.numOfKeptEntries() > int64(mads.idxTree.ActiveCount())*KeptEntriesToActiveEntriesRatio &&
		int64(mads.idxTree.ActiveCount()) > StartReapThres {
		twigID := mads.meta.GetOldestActiveTwigID()
		entryBzChan := mads.datTree.GetActiveEntriesInTwig(twigID)
		for entryBz := range entryBzChan {
			sn := datatree.ExtractSerialNum(entryBz)
			mads.DeactiviateEntry(sn)
			sn = mads.meta.GetMaxSerialNum()
			datatree.UpdateSerialNum(entryBz, sn)
			mads.meta.IncrMaxSerialNum()
			key := datatree.ExtractKeyFromRawBytes(entryBz)
			if string(key) == "dummy" {
				panic(fmt.Sprintf("dummy entry cannot be active %d", sn))
			}
			pos := mads.datTree.AppendEntryRawBytes(entryBz, sn)
			//if len(key) != 8 {
			//	e := datatree.EntryFromRawBytes(entryBz)
			//	fmt.Printf("key %#v e %#v\n", key, e)
			//}
			mads.idxTree.Set(key, pos)
		}
		mads.datTree.EvictTwig(twigID)
		mads.meta.IncrOldestActiveTwigID()
	}
	//fmt.Printf("end numOfKeptEntries %d ActiveCount %d x2 %d\n", mads.numOfKeptEntries(), mads.idxTree.ActiveCount(), mads.idxTree.ActiveCount()*2)
	if mads.datTree.DeactivedSNListSize() != 0 {
		mads.flushDeactivedSNList()
	}
	rootHash := mads.datTree.EndBlock()
	mads.meta.SetRootHash(rootHash)
	mads.k2heMap = NewBucketMap(heMapSize) // clear content
	mads.k2nkMap = NewBucketMap(nkMapSize) // clear content
	for i := range mads.tempEntries64 {
		mads.tempEntries64[i] = mads.tempEntries64[i][:0] // clear content
	}

	eS, tS := mads.datTree.GetFileSizes()
	mads.meta.SetEntryFileSize(eS)
	mads.meta.SetTwigMtFileSize(tS)
	mads.datTree.WaitForFlushing()
	//fmt.Println("Upper Tree At EndWrite"); mads.datTree.PrintTree()
	mads.meta.Commit()
	mads.idxTree.EndWrite()
	mads.rocksdb.CloseOldBatch()
}

func (mads *MoeingADS) initGuards() {
	mads.idxTree.BeginWrite(-1)
	mads.meta.SetCurrHeight(-1)

	entry := &Entry{
		Key:        mads.startKey,
		Value:      []byte{},
		NextKey:    mads.endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  mads.meta.GetMaxSerialNum(),
	}
	pos := mads.datTree.AppendEntry(entry)
	mads.meta.IncrMaxSerialNum()
	mads.idxTree.Set(mads.startKey, pos)

	entry = &Entry{
		Key:        mads.endKey,
		Value:      []byte{},
		NextKey:    mads.endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  mads.meta.GetMaxSerialNum(),
	}
	pos = mads.datTree.AppendEntry(entry)
	mads.meta.IncrMaxSerialNum()
	mads.idxTree.Set(mads.endKey, pos)

	mads.idxTree.EndWrite()
	rootHash := mads.datTree.EndBlock()
	mads.meta.SetRootHash(rootHash)
	mads.meta.Commit()
	mads.rocksdb.CloseOldBatch()
	mads.rocksdb.OpenNewBatch()
}

func (mads *MoeingADS) PruneBeforeHeight(height int64) {
	start := mads.meta.GetLastPrunedTwig() + 1
	end := start + 1
	endHeight := mads.meta.GetTwigHeight(end)
	if endHeight < 0 {
		return
	}
	for endHeight < height && mads.datTree.TwigCanBePruned(end) {
		end++
		endHeight = mads.meta.GetTwigHeight(end)
		if endHeight < 0 {
			return
		}
	}
	end--
	if end > start {
		edgeNodesBytes := mads.datTree.PruneTwigs(start, end)
		mads.meta.SetEdgeNodes(edgeNodesBytes)
		for i := start; i < end; i++ {
			mads.meta.DeleteTwigHeight(i)
		}
		mads.meta.SetLastPrunedTwig(end - 1)
	}
	mads.rocksdb.SetPruneHeight(uint64(height))
}

type BucketMap struct {
	maps [64]map[string]*HotEntry
	mtxs [64]sync.RWMutex
}

func NewBucketMap(size int) *BucketMap {
	res := &BucketMap{}
	for i := range res.maps {
		res.maps[i] = make(map[string]*HotEntry, size)
	}
	return res
}

func (bm *BucketMap) Load(key string) (value *HotEntry, ok bool) {
	idx := int(key[0] >> 2) // most significant 6 bits as index
	bm.mtxs[idx].RLock()
	defer bm.mtxs[idx].RUnlock()
	value, ok = bm.maps[idx][key]
	return
}

func (bm *BucketMap) Store(key string, value *HotEntry) {
	idx := int(key[0] >> 2) // most significant 6 bits as index
	bm.mtxs[idx].Lock()
	defer bm.mtxs[idx].Unlock()
	bm.maps[idx][key] = value
}

type Iterator struct {
	mads *MoeingADS
	iter types.IteratorUI64
}

var _ types.Iterator = (*Iterator)(nil)

func (iter *Iterator) Domain() (start []byte, end []byte) {
	return iter.iter.Domain()
}
func (iter *Iterator) Valid() bool {
	return iter.iter.Valid()
}
func (iter *Iterator) Next() {
	iter.iter.Next()
}
func (iter *Iterator) Key() []byte {
	return iter.iter.Key()
}
func (iter *Iterator) Value() []byte {
	if !iter.Valid() {
		return nil
	}
	pos := iter.iter.Value()
	//fmt.Printf("pos = %d %#v\n", pos, iter.mads.datTree.ReadEntry(int64(pos)))
	return iter.mads.datTree.ReadEntry(int64(pos)).Value
}
func (iter *Iterator) Close() {
	iter.iter.Close()
}

func (mads *MoeingADS) Iterator(start, end []byte) types.Iterator {
	return &Iterator{mads: mads, iter: mads.idxTree.Iterator(start, end)}
}

func (mads *MoeingADS) ReverseIterator(start, end []byte) types.Iterator {
	return &Iterator{mads: mads, iter: mads.idxTree.ReverseIterator(start, end)}
}
