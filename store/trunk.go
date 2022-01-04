package store

import (
	"sync/atomic"

	"github.com/dterei/gotsc"

	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/store/types"
	adstypes "github.com/smartbch/moeingads/types"
)

var PhaseTrunkTime, PhaseEndWriteTime, tscOverhead uint64 //nolint:unused

func init() {
	tscOverhead = gotsc.TSCOverhead()
}

// TrunkStore use a temporal cache to buffer the written data from
// successive transactions, such that the latter transactions can read
// the data which were written by previous transactions.
// When these transactions are running, isWriting is always 0.
// When a transaction is writting back to TrunkStore, isWriting is setting to 1.
// When the writting process is finished, isWritting is setting back to 0.
// The client code should manages the transactions to make sure their writing-back
// processes are performed in serial.
// When a TrunkStore closes after servicing all the transactions, it may write
// the temporal cache's content to the underlying RootStore, if isReadOnly==false.
// A transaction usually access a TrunkStore through a RabbitStore.
type TrunkStore struct {
	cache      *CacheStore
	root       types.RootStoreI
	isWriting  int64
	isReadOnly bool
}

func (ts *TrunkStore) Get(key []byte) []byte {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	res, status := ts.cache.Get(key)
	switch status {
	case types.JustDeleted:
		return nil
	case types.Hit:
		newRes := make([]byte, len(res))
		copy(newRes, res)
		return newRes
	case types.Missed:
		return ts.root.Get(key)
	default:
		panic("Invalid Status")
	}
}

func (ts *TrunkStore) RLock() {
	ts.root.RLock()
}

func (ts *TrunkStore) RUnlock() {
	ts.root.RUnlock()
}

func (ts *TrunkStore) CacheSize() int {
	return ts.cache.Size()
}

func (ts *TrunkStore) GetAtHeight(key []byte, height uint64) []byte {
	return ts.root.GetAtHeight(key, height)
}

func (ts *TrunkStore) PrepareForUpdate(key []byte) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	ts.root.PrepareForUpdate(key)
}

func (ts *TrunkStore) PrepareForDeletion(key []byte) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	ts.root.PrepareForDeletion(key)
}

func (ts *TrunkStore) Update(updater func(db types.SetDeleter)) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	updater(ts.cache)
}

func (ts *TrunkStore) writeBack() {
	//@ start := gotsc.BenchStart()
	if atomic.AddInt64(&ts.isWriting, 1) != 1 {
		panic("Conflict During Writing")
	}
	ts.root.BeginWrite()

	datatree.ParallelRun(adstypes.ShardCount, func(shardID int) {
		ts.cache.ScanAllEntriesInShard(shardID, func(key, value []byte, isDeleted bool) {
			if isDeleted {
				ts.root.Delete(key)
			} else {
				ts.root.Set(key, value)
			}
		})
	})
	//@ PhaseTrunkTime += gotsc.BenchEnd() - start - tscOverhead
	//@ start = gotsc.BenchStart()
	ts.root.EndWrite()
	//@ PhaseEndWriteTime += gotsc.BenchEnd() - start - tscOverhead
	if atomic.AddInt64(&ts.isWriting, -1) != 0 {
		panic("Conflict During Writing")
	}
}

func (ts *TrunkStore) WriteBack() {
	ts.root.Lock()
	ts.writeBack()
	ts.root.Unlock()
}

func (ts *TrunkStore) Close(writeBack bool) {
	if ts.isReadOnly && writeBack {
		panic("A Readonly TrunkStore cannot be written back")
	}
	if writeBack {
		ts.WriteBack()
	}
	ts.cache.Close()
	ts.cache = nil
	ts.root = nil
}

func (ts *TrunkStore) ActiveCount() int {
	return ts.root.ActiveCount()
}
