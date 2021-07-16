package moeingads

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	//"github.com/dterei/gotsc"
	sha256 "github.com/minio/sha256-simd"

	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/indextree"
	"github.com/smartbch/moeingads/metadb"
	"github.com/smartbch/moeingads/types"
)

const (
	KeptEntriesToActiveEntriesRatio = 2

	heMapSize   = 128
	nkSetSize   = 64
	BucketCount = 64
	JobChanSize = 256

	MinDeactiveEntries = 100 //minimum deactived entry count in each round of compaction
	MinKeptTwigs       = 10  //minimum kept twig count in each shard
)

var Phase1n2Time, Phase1Time, Phase2Time, Phase3Time, Phase4Time, Phase0Time, tscOverhead uint64

type MoeingADS struct {
	meta           types.MetaDB
	idxTree        types.IndexTree
	datTree        [types.ShardCount]types.DataTree
	rocksdb        *indextree.RocksDB
	k2heMap        *BucketMap // key-to-hot-entry map
	nkSet          *BucketSet // next-key set
	tempEntries64  [BucketCount][]*HotEntry
	cachedEntries  []*HotEntry
	startKey       []byte
	endKey         []byte
	idxTreeJobChan [types.IndexChanCount]chan idxTreeJob
	idxTreeJobWG   sync.WaitGroup
}

func NewMoeingADS4Mock(startEndKeys [][]byte) *MoeingADS {
	mads := &MoeingADS{
		k2heMap:  NewBucketMap(repeat64(heMapSize)),
		nkSet:    NewBucketSet(repeat64(nkSetSize)),
		startKey: append([]byte{}, startEndKeys[0]...),
		endKey:   append([]byte{}, startEndKeys[1]...),
	}

	for i := 0; i < types.ShardCount; i++ {
		mads.datTree[i] = datatree.NewMockDataTree()
	}
	for i := 0; i < types.IndexChanCount; i++ {
		mads.idxTreeJobChan[i] = make(chan idxTreeJob, JobChanSize)
	}
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

func NewMoeingADS(dirName string, canQueryHistory bool /*not supported yet*/, startEndKeys [][]byte) (*MoeingADS, error) {
	//tscOverhead = gotsc.TSCOverhead()
	_, err := os.Stat(dirName)
	dirNotExists := os.IsNotExist(err)
	mads := &MoeingADS{
		k2heMap:       NewBucketMap(repeat64(heMapSize)),
		nkSet:         NewBucketSet(repeat64(nkSetSize)),
		cachedEntries: make([]*HotEntry, 0, 2000),
		startKey:      append([]byte{}, startEndKeys[0]...),
		endKey:        append([]byte{}, startEndKeys[1]...),
	}
	for i := 0; i < types.IndexChanCount; i++ {
		mads.idxTreeJobChan[i] = make(chan idxTreeJob, JobChanSize)
	}
	for i := range mads.tempEntries64 {
		mads.tempEntries64[i] = make([]*HotEntry, 0, len(mads.cachedEntries)/8)
	}
	if dirNotExists {
		os.Mkdir(dirName, 0700)
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

	mads.idxTree = indextree.NewNVTreeMem(nil)
	if dirNotExists { // Create a new database in this dir
		for i := 0; i < types.ShardCount; i++ {
			suffix := fmt.Sprintf(".%d", i)
			mads.datTree[i] = datatree.NewEmptyTree(datatree.BufferSize, defaultFileSize, dirName, suffix)
		}
		mads.rocksdb.OpenNewBatch()
		mads.meta.Init()
		for i := 0; i < types.ShardCount; i++ {
			for j := 0; j < 2048; j++ {
				sn := mads.meta.GetMaxSerialNum(i)
				mads.meta.IncrMaxSerialNum(i)
				entry := datatree.DummyEntry(sn)
				mads.datTree[i].AppendEntry(entry)
			}
		}
		mads.initGuards()
		mads.rocksdb.CloseOldBatch()
	} else {
		mads.recoverDataTrees(dirName)
		mads.idxTree.BeginWrite(0) // we set height=0 here, but this value will not be used
		mads.runIdxTreeJobs()
		datatree.ParallelRun(types.ShardCount, func(shardID int) {
			oldestActiveTwigID := mads.meta.GetOldestActiveTwigID(shardID)
			keyAndPosChan := make(chan types.KeyAndPos, JobChanSize)
			go mads.datTree[shardID].ScanEntriesLite(oldestActiveTwigID, keyAndPosChan)
			for e := range keyAndPosChan {
				if mads.datTree[shardID].GetActiveBit(e.SerialNum) {
					chanID := types.GetIndexChanID(e.Key[0])
					mads.idxTreeJobChan[chanID] <- idxTreeJob{key: e.Key, pos: e.Pos}
				}
			}
		})
		mads.flushIdxTreeJobs()
		mads.idxTree.EndWrite()
	}

	return mads, nil
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
		SerialNum:  mads.meta.GetMaxSerialNum(0),
	}
	pos := mads.datTree[0].AppendEntry(entry)
	mads.meta.IncrMaxSerialNum(0)
	mads.idxTree.Set(mads.startKey, pos)

	lastShard := types.ShardCount - 1
	entry = &Entry{
		Key:        mads.endKey,
		Value:      []byte{},
		NextKey:    mads.endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  mads.meta.GetMaxSerialNum(lastShard),
	}
	pos = mads.datTree[lastShard].AppendEntry(entry)
	mads.meta.IncrMaxSerialNum(lastShard)
	mads.idxTree.Set(mads.endKey, pos)

	mads.idxTree.EndWrite()
	mads.allShardEndBlock()
	mads.meta.Commit()
	for i := 0; i < types.ShardCount; i++ {
		mads.datTree[i].WaitForFlushing()
	}
	mads.rocksdb.CloseOldBatch()
	mads.rocksdb.OpenNewBatch()
}

func (mads *MoeingADS) recoverDataTrees(dirName string) {
	datatree.ParallelRun(types.ShardCount, func(shardID int) {
		oldestActiveTwigID := mads.meta.GetOldestActiveTwigID(shardID)
		youngestTwigID := mads.meta.GetYoungestTwigID(shardID)
		mads.meta.GetYoungestTwigID(shardID)
		bz := mads.meta.GetEdgeNodes(shardID)
		edgeNodes := datatree.BytesToEdgeNodes(bz)
		var recoveredRoot [32]byte
		suffix := fmt.Sprintf(".%d", shardID)
		mads.datTree[shardID], recoveredRoot = datatree.RecoverTree(datatree.BufferSize, defaultFileSize,
			dirName, suffix, edgeNodes, mads.meta.GetLastPrunedTwig(shardID),
			oldestActiveTwigID, youngestTwigID,
			[]int64{mads.meta.GetEntryFileSize(shardID), mads.meta.GetTwigMtFileSize(shardID)})
		recordedRoot := mads.meta.GetRootHash(shardID)
		if recordedRoot != recoveredRoot {
			fmt.Printf("%#v %#v\n", recordedRoot, recoveredRoot)
			panic("Failed To Recover")
		}
	})
}

func (mads *MoeingADS) PrintMetaInfo() {
	mads.meta.PrintInfo()
}

func (mads *MoeingADS) PrintIdxTree() {
	iter := mads.idxTree.Iterator(mads.startKey, mads.endKey)
	defer iter.Close()
	for iter.Valid() {
		fmt.Printf("@ %v %d\n", iter.Key(), iter.Value())
		iter.Next()
	}
}

func (mads *MoeingADS) Close() {
	mads.idxTree.Close()
	mads.rocksdb.Close()
	for i := 0; i < types.ShardCount; i++ {
		mads.datTree[i].Close()
		mads.datTree[i] = nil
	}
	mads.meta.Close()
	mads.idxTree = nil
	mads.rocksdb = nil
	mads.meta = nil
	mads.k2heMap = nil
	mads.nkSet = nil
}

type Entry = types.Entry
type HotEntry = types.HotEntry

func (mads *MoeingADS) GetRootHash() []byte {
	var n8 [8][32]byte
	var n4 [4][32]byte
	var n2 [2][32]byte
	for i := range n8 {
		n8[i] = mads.meta.GetRootHash(i)
	}
	n4[0] = sha256.Sum256(append(n8[0][:], n8[1][:]...))
	n4[1] = sha256.Sum256(append(n8[2][:], n8[3][:]...))
	n4[2] = sha256.Sum256(append(n8[4][:], n8[5][:]...))
	n4[3] = sha256.Sum256(append(n8[6][:], n8[7][:]...))
	n2[0] = sha256.Sum256(append(n4[0][:], n4[1][:]...))
	n2[1] = sha256.Sum256(append(n4[2][:], n4[3][:]...))
	n1 := sha256.Sum256(append(n2[0][:], n2[1][:]...))
	return n1[:]
}

func (mads *MoeingADS) GetProof(k []byte) (*Entry, []byte, error) {
	e := mads.GetEntry(k)
	if e != nil {
		shardID := types.GetShardID(k)
		bz, err := mads.datTree[shardID].GetProofBytes(e.SerialNum)
		return e, bz, err
	}
	return nil, nil, errors.New("Cannot find entry")
}

func (mads *MoeingADS) GetEntry(k []byte) *Entry {
	pos, ok := mads.idxTree.Get(k)
	if !ok {
		return nil
	}
	shardID := types.GetShardID(k)
	e := mads.datTree[shardID].ReadEntry(int64(pos))
	if !mads.datTree[shardID].GetActiveBit(e.SerialNum) {
		panic(fmt.Sprintf("Reading inactive entry %d\n", e.SerialNum))
	}
	return e
}

// An entry is prepared for insertion, but not inserted (no SerialNum and Value assigned)
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
	pos, findIt := mads.idxTree.Get(k)
	if findIt { // The case of Change
		shardID := types.GetShardID(k)
		entry := mads.datTree[shardID].ReadEntry(int64(pos))
		mads.k2heMap.Store(string(k), &HotEntry{
			EntryPtr:  entry,
			Operation: types.OpNone, // may be changed to meaningful value after 'BeginWrite'
		})
		return
	}
	prevEntry := mads.getPrevEntry(k)

	// The case of Insert
	mads.k2heMap.Store(string(k), &HotEntry{
		EntryPtr: &Entry{
			Key:        append([]byte{}, k...),
			Value:      nil,
			NextKey:    nil,
			Height:     0,
			LastHeight: 0,
			SerialNum:  -1, //inserted entries has negative SerialNum
		},
		Operation: types.OpNone, // may be changed to meaningful value after 'BeginWrite'
	})

	mads.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone, // may be changed to meaningful value after 'BeginWrite'
	})

	mads.nkSet.Store(string(prevEntry.NextKey))
}

func (mads *MoeingADS) PrepareForDeletion(k []byte) (findIt bool) {
	pos, findIt := mads.idxTree.Get(k)
	if !findIt {
		return
	}

	shardID := types.GetShardID(k)
	entry := mads.datTree[shardID].ReadEntry(int64(pos))
	prevEntry := mads.getPrevEntry(k)

	mads.k2heMap.Store(string(entry.Key), &HotEntry{
		EntryPtr:  entry,
		Operation: types.OpNone, // may be changed to meaningful value after 'BeginWrite'
	})

	mads.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone, // may be changed to meaningful value after 'BeginWrite'
	})

	mads.nkSet.Store(string(entry.NextKey))
	return
}

// Hint entries are just used to mark next keys in tempEntries64. They have no real value.
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
	prevKey := iter.Key()
	shardID := types.GetShardID(prevKey)
	e := mads.datTree[shardID].ReadEntry(int64(pos))
	if !mads.datTree[shardID].GetActiveBit(e.SerialNum) {
		panic(fmt.Sprintf("Reading inactive entry %d\n", e.SerialNum))
	}
	return e
}

func (mads *MoeingADS) numOfKeptEntries() (sum int64) {
	for i := 0; i < types.ShardCount; i++ {
		sum += mads.meta.GetMaxSerialNum(i) - mads.meta.GetOldestActiveTwigID(i)*datatree.LeafCountInTwig
	}
	return
}

func (mads *MoeingADS) GetCurrHeight() int64 {
	return mads.meta.GetCurrHeight()
}

func (mads *MoeingADS) BeginWrite(height int64) {
	mads.rocksdb.OpenNewBatch()
	mads.idxTree.BeginWrite(height)
	mads.meta.SetCurrHeight(height)
}

// Modify a KV pair. It is shard-safe
func (mads *MoeingADS) Set(key, value []byte) {
	hotEntry, ok := mads.k2heMap.Load(string(key))
	if !ok {
		panic(fmt.Sprintf("Can not find entry in cache, key=%#v", key))
	}
	if hotEntry == nil {
		panic("Can not change or insert at a fake entry")
	}
	hotEntry.EntryPtr.Value = value
	hotEntry.Operation = types.OpInsertOrChange
}

// Delete a KV pair. It is shard-safe
func (mads *MoeingADS) Delete(key []byte) {
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
		panic("Can not find next entry")
	}
	return j
}

type idxTreeJob struct {
	key []byte
	pos int64
}

func (mads *MoeingADS) runIdxTreeJobs() {
	mads.idxTreeJobWG.Add(len(mads.idxTreeJobChan))
	for i := range mads.idxTreeJobChan {
		go func(chanID int) {
			for {
				job := <-mads.idxTreeJobChan[chanID]
				if job.key == nil {
					break
				}
				if job.pos < 0 {
					mads.idxTree.Delete(job.key)
				} else {
					mads.idxTree.Set(job.key, job.pos)
				}
			}
			mads.idxTreeJobWG.Done()
		}(i)
	}
}

func (mads *MoeingADS) flushIdxTreeJobs() {
	for i := range mads.idxTreeJobChan {
		mads.idxTreeJobChan[i] <- idxTreeJob{key: nil} // end of job
	}
	mads.idxTreeJobWG.Wait()
}

func (mads *MoeingADS) update() {
	sharedIdx := int64(-1)
	datatree.ParallelRun(runtime.NumCPU(), func(_ int) {
		for { // we dump data into tempEntries64 from k2heMap and nkSet, for sorting them
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= BucketCount {
				break
			}
			for _, e := range mads.k2heMap.maps[myIdx] {
				mads.tempEntries64[myIdx] = append(mads.tempEntries64[myIdx], e)
			}
			for k := range mads.nkSet.maps[myIdx] {
				if _, ok := mads.k2heMap.maps[myIdx][k]; ok {
					continue // we have added it because it's in k2heMap
				}
				mads.tempEntries64[myIdx] = append(mads.tempEntries64[myIdx], makeHintHotEntry(k))
			}
			entries := mads.tempEntries64[myIdx]
			sort.Slice(entries, func(i, j int) bool {
				return bytes.Compare(entries[i].EntryPtr.Key, entries[j].EntryPtr.Key) < 0
			})
		}
	})
	mads.cachedEntries = mads.cachedEntries[:0]
	for _, entries := range mads.tempEntries64 { // merge data into cachedEntries from tempEntries64
		mads.cachedEntries = append(mads.cachedEntries, entries...)
	}
	// set NextKey to correct values and mark IsModified
	for i, hotEntry := range mads.cachedEntries {
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
			next := getNext(mads.cachedEntries, i)
			hotEntry.EntryPtr.NextKey = mads.cachedEntries[next].EntryPtr.Key
			prev := getPrev(mads.cachedEntries, i)
			mads.cachedEntries[prev].EntryPtr.NextKey = hotEntry.EntryPtr.Key
			mads.cachedEntries[prev].IsTouchedByNext = true
		} else if isModified(hotEntry) {
			hotEntry.IsModified = true
		}
	}
	debugPanic(2)
	// update stored data
	mads.runIdxTreeJobs()
	datatree.ParallelRun(types.ShardCount, func(shardID int) {
		for _, hotEntry := range mads.cachedEntries {
			ptr := hotEntry.EntryPtr
			id := types.GetShardID(ptr.Key)
			if id != shardID {
				continue
			} else if id > shardID {
				break
			}
			if !(hotEntry.IsModified || hotEntry.IsTouchedByNext) {
				continue
			}
			chanID := types.GetIndexChanID(ptr.Key[0])
			if hotEntry.Operation == types.OpDelete && ptr.SerialNum >= 0 {
				// if ptr.SerialNum==-1, then we are deleting a just-inserted value, so ignore it.
				mads.DeactiviateEntry(shardID, ptr.SerialNum)
				mads.idxTreeJobChan[chanID] <- idxTreeJob{key: ptr.Key, pos: -1} //delete
			} else if hotEntry.Operation != types.OpNone || hotEntry.IsTouchedByNext {
				if ptr.SerialNum >= 0 { // if this entry already exists
					mads.DeactiviateEntry(shardID, ptr.SerialNum)
				}
				ptr.LastHeight = ptr.Height
				ptr.Height = mads.meta.GetCurrHeight()
				ptr.SerialNum = mads.meta.GetMaxSerialNum(shardID)
				mads.meta.IncrMaxSerialNum(shardID)
				pos := mads.datTree[shardID].AppendEntry(ptr)
				mads.idxTreeJobChan[chanID] <- idxTreeJob{key: ptr.Key, pos: pos} //update
			}
		}
	})
	mads.flushIdxTreeJobs()
}

func (mads *MoeingADS) DeactiviateEntry(shardID int, sn int64) {
	pendingDeactCount := mads.datTree[shardID].DeactiviateEntry(sn)
	if pendingDeactCount > datatree.DeactivedSNListMaxLen {
		mads.flushDeactivedSNList(shardID)
	}
}

func (mads *MoeingADS) flushDeactivedSNList(shardID int) {
	sn := mads.meta.GetMaxSerialNum(shardID)
	mads.meta.IncrMaxSerialNum(shardID)
	entry := datatree.DummyEntry(sn)
	mads.datTree[shardID].DeactiviateEntry(sn)
	mads.datTree[shardID].AppendEntry(entry)
}

func (mads *MoeingADS) CheckConsistency() {
	iter := mads.idxTree.ReverseIterator(mads.startKey, mads.endKey)
	defer iter.Close()
	nextKey := mads.endKey
	for iter.Valid() && !bytes.Equal(iter.Key(), mads.startKey) {
		pos := iter.Value()
		key := iter.Key()
		shardID := types.GetShardID(key)
		entry := mads.datTree[shardID].ReadEntry(int64(pos))
		if !bytes.Equal(entry.NextKey, nextKey) {
			panic(fmt.Sprintf("Invalid NextKey for %#v, datTree %#v, idxTree %#v\n",
				iter.Key(), entry.NextKey, nextKey))
		}
		nextKey = iter.Key()
		iter.Next()
	}
}

func (mads *MoeingADS) ScanAll(fn func(key, value []byte)) {
	iter := mads.idxTree.ReverseIterator(mads.startKey, mads.endKey)
	defer iter.Close()
	for iter.Valid() && !bytes.Equal(iter.Key(), mads.startKey) {
		pos := iter.Value()
		key := iter.Key()
		shardID := types.GetShardID(key)
		entry := mads.datTree[shardID].ReadEntry(int64(pos))
		fn(key, entry.Value)
		iter.Next()
	}
}

func (mads *MoeingADS) ActiveCount() int {
	return mads.idxTree.ActiveCount()
}

func (mads *MoeingADS) compactForShard(shardID int) {
	twigID := mads.meta.GetOldestActiveTwigID(shardID)
	if twigID+MinKeptTwigs > mads.meta.GetYoungestTwigID(shardID) {
		return
	}
	entryBzChan := mads.datTree[shardID].GetActiveEntriesInTwig(twigID)
	for entryBz := range entryBzChan {
		sn := datatree.ExtractSerialNum(entryBz)
		mads.DeactiviateEntry(shardID, sn)
		sn = mads.meta.GetMaxSerialNum(shardID)
		datatree.UpdateSerialNum(entryBz, sn)
		mads.meta.IncrMaxSerialNum(shardID)
		key := datatree.ExtractKeyFromRawBytes(entryBz)
		if string(key) == "dummy" {
			panic(fmt.Sprintf("dummy entry cannot be active %d", sn))
		}
		pos := mads.datTree[shardID].AppendEntryRawBytes(entryBz, sn)
		chanID := types.GetIndexChanID(key[0])
		mads.idxTreeJobChan[chanID] <- idxTreeJob{key: key, pos: pos} //update
	}
	mads.datTree[shardID].EvictTwig(twigID)
	mads.meta.IncrOldestActiveTwigID(shardID)
}

func (mads *MoeingADS) allShardEndBlock() {
	for i := 0; i < types.ShardCount; i++ {
		if mads.datTree[i].DeactivedSNListSize() != 0 {
			mads.flushDeactivedSNList(i)
		}
		rootHash := mads.datTree[i].EndBlock()
		mads.meta.SetRootHash(i, rootHash)
		entryFileSize, twigMtFileSize := mads.datTree[i].GetFileSizes()
		mads.meta.SetEntryFileSize(i, entryFileSize)
		mads.meta.SetTwigMtFileSize(i, twigMtFileSize)
	}
}

func (mads *MoeingADS) EndWrite() {
	debugPanic(1)
	mads.update()
	debugPanic(3)
	activeCount := int64(mads.idxTree.ActiveCount())
	for {
		if activeCount < StartReapThres {
			break
		}
		if mads.numOfKeptEntries() < activeCount*KeptEntriesToActiveEntriesRatio {
			break
		}
		mads.runIdxTreeJobs()
		datatree.ParallelRun(types.ShardCount, func(shardID int) {
			mads.compactForShard(shardID)
		})
		mads.flushIdxTreeJobs()
		newActiveCount := int64(mads.idxTree.ActiveCount())
		if newActiveCount > activeCount-MinDeactiveEntries {
			break
		}
		activeCount = newActiveCount
	}
	mads.allShardEndBlock()
	mads.k2heMap = NewBucketMap(mads.k2heMap.GetSizes()) // clear content
	mads.nkSet = NewBucketSet(mads.nkSet.GetSizes())     // clear content
	for i := range mads.tempEntries64 {
		mads.tempEntries64[i] = mads.tempEntries64[i][:0] // clear content
	}

	for i := 0; i < types.ShardCount; i++ {
		mads.datTree[i].WaitForFlushing()
	}
	debugPanic(4)
	mads.meta.Commit()
	mads.idxTree.EndWrite()
	mads.rocksdb.CloseOldBatch()
}

func (mads *MoeingADS) PruneBeforeHeight(height int64) {
	var starts, ends [types.ShardCount]int64
	datatree.ParallelRun(types.ShardCount, func(shardID int) {
		start := mads.meta.GetLastPrunedTwig(shardID) + 1
		end := start + 1
		for {
			endHeight := mads.meta.GetTwigHeight(shardID, end)
			if endHeight < 0 || endHeight >= height || !mads.datTree[shardID].TwigCanBePruned(end) {
				break
			}
			end++
		}
		end--
		starts[shardID], ends[shardID] = start, end
	})
	mads.rocksdb.OpenNewBatch()
	for shardID := 0; shardID < types.ShardCount; shardID++ {
		start, end := starts[shardID], ends[shardID]
		if end > start+datatree.MinPruneCount {
			edgeNodesBytes := mads.datTree[shardID].PruneTwigs(start, end)
			mads.meta.SetEdgeNodes(shardID, edgeNodesBytes)
			for twig := start; twig < end; twig++ {
				mads.meta.DeleteTwigHeight(shardID, twig)
			}
			mads.meta.SetLastPrunedTwig(shardID, end-1)
		}
	}
	mads.rocksdb.CloseOldBatch()
	mads.rocksdb.SetPruneHeight(uint64(height))
}

func (mads *MoeingADS) CheckHashConsistency() {
	for i := range mads.datTree {
		tree, ok := mads.datTree[i].(*datatree.Tree)
		if ok {
			datatree.CheckHashConsistency(tree)
		}
	}
}

func repeat64(v int) []int {
	var buf [BucketCount]int
	for i := range buf {
		buf[i] = v
	}
	return buf[:]
}

type BucketMap struct {
	maps [BucketCount]map[string]*HotEntry
	mtxs [BucketCount]sync.RWMutex
}

func NewBucketMap(sizeList []int) *BucketMap {
	res := &BucketMap{}
	for i := range res.maps {
		res.maps[i] = make(map[string]*HotEntry, sizeList[i])
	}
	return res
}

func (bm *BucketMap) GetSizes() []int {
	var buf [BucketCount]int
	for i, m := range bm.maps {
		buf[i] = len(m)
	}
	return buf[:]
}

func (bm *BucketMap) Load(key string) (value *HotEntry, ok bool) {
	idx := int(key[0] >> 2) // most significant 6 bits as bucket index
	bm.mtxs[idx].RLock()
	defer bm.mtxs[idx].RUnlock()
	value, ok = bm.maps[idx][key]
	return
}

func (bm *BucketMap) Store(key string, value *HotEntry) {
	idx := int(key[0] >> 2) // most significant 6 bits as bucket index
	bm.mtxs[idx].Lock()
	defer bm.mtxs[idx].Unlock()
	bm.maps[idx][key] = value
}

type BucketSet struct {
	maps [BucketCount]map[string]struct{}
	mtxs [BucketCount]sync.RWMutex
}

func NewBucketSet(sizeList []int) *BucketSet {
	res := &BucketSet{}
	for i := range res.maps {
		res.maps[i] = make(map[string]struct{}, sizeList[i])
	}
	return res
}

func (bs *BucketSet) GetSizes() []int {
	var buf [BucketCount]int
	for i, m := range bs.maps {
		buf[i] = len(m)
	}
	return buf[:]
}

func (bs *BucketSet) Store(key string) {
	idx := int(key[0] >> 2) // most significant 6 bits as bucket index
	bs.mtxs[idx].Lock()
	defer bs.mtxs[idx].Unlock()
	bs.maps[idx][key] = struct{}{}
}
