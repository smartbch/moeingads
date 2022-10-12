package indextree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/smartbch/moeingads/indextree/b"
	"github.com/smartbch/moeingads/types"
)

const (
	MaxKeyLength     = 8192
	RecentBlockCount = 1024
)

type Iterator = types.IteratorUI64

var _ Iterator = (*ForwardIterMem)(nil)
var _ Iterator = (*BackwardIterMem)(nil)

type ForwardIterMem struct {
	enumerator *b.Enumerator
	tree       *NVTreeMem
	start      uint64
	end        uint64
	key        uint64
	value      int64
	err        error
}
type BackwardIterMem struct {
	enumerator *b.Enumerator
	tree       *NVTreeMem
	start      uint64
	end        uint64
	key        uint64
	value      int64
	err        error
}

func (iter *ForwardIterMem) Domain() ([]byte, []byte) {
	return u64ToBz(iter.start), u64ToBz(iter.end)
}
func (iter *ForwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *ForwardIterMem) Next() {
	if iter.tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Next()
		if iter.key >= iter.end {
			iter.err = io.EOF
		}
	}
}
func (iter *ForwardIterMem) Key() []byte {
	return u64ToBz(iter.key)
}
func (iter *ForwardIterMem) Value() int64 {
	return iter.value
}
func (iter *ForwardIterMem) Close() {
	iter.tree.mtx.RUnlock() // It was locked when iter was created
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}

func (iter *BackwardIterMem) Domain() ([]byte, []byte) {
	return u64ToBz(iter.start), u64ToBz(iter.end)
}
func (iter *BackwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *BackwardIterMem) Next() {
	if iter.tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Prev()
		if iter.key < iter.start {
			iter.err = io.EOF
		}
	}
}
func (iter *BackwardIterMem) Key() []byte {
	return u64ToBz(iter.key)
}
func (iter *BackwardIterMem) Value() int64 {
	return iter.value
}
func (iter *BackwardIterMem) Close() {
	iter.tree.mtx.RUnlock() // It was locked when iter was created
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}

/* ============================
Here we implement IndexTree with an in-memory B-Tree and a on-disk RocksDB.
The B-Tree contains only the latest key-position records, while the RocksDB
contains several versions of positions for each key. The keys in RocksDB have
two parts: the original key and 64-bit height. The height means the key-position
record is created (get valid) at this height.
*/

type NVTreeMem struct {
	mtx        sync.RWMutex
	bt         *b.Tree
	isWriting  bool
	rocksdb    *RocksDB
	currHeight [8]byte
	duringInit bool

	recentCache   *RecentCache
	currHeightI64 int64
}

var _ types.IndexTree = (*NVTreeMem)(nil)

func NewNVTreeMem(rocksdb *RocksDB) *NVTreeMem {
	btree := b.TreeNew()
	return &NVTreeMem{
		bt:          btree,
		rocksdb:     rocksdb,
		recentCache: NewRecentCache(),
	}
}

// Load the RocksDB and use its up-to-date records to initialize the in-memory B-Tree.
// RocksDB's historical records are ignored.
func (tree *NVTreeMem) Init(repFn func([]byte)) (err error) {
	iter := tree.rocksdb.ReverseIterator([]byte{}, []byte(nil))
	defer iter.Close()
	var key []byte
	for iter.Valid() {
		k := iter.Key()
		v := iter.Value()
		if k[0] != 0 {
			continue // the first byte must be zero
		}
		k = k[1:]
		if repFn != nil {
			repFn(k) // to report the progress
		}
		if len(k) < 8 {
			panic("key length is too short")
		}
		if len(v) != 8 && len(v) != 0 {
			panic("value length is not 8 or 0")
		}
		if !bytes.Equal(k[0:len(k)-8], key) {
			//write the up-to-date value
			key = append([]byte{}, k[0:len(k)-8]...)
			if len(v) != 0 {
				tree.bt.Set(binary.BigEndian.Uint64(key), int64(binary.BigEndian.Uint64(v)))
			}
		}
		iter.Next()
	}
	return nil
}

func (tree *NVTreeMem) SetDuringInit(b bool) {
	tree.duringInit = b
}

func (tree *NVTreeMem) Close() {
	tree.bt.Close()
	// tree.rocksdb should be closed at somewhere else, not here
}

func (tree *NVTreeMem) ActiveCount() int {
	return tree.bt.Len()
}

// Begin the write phase of block execution
func (tree *NVTreeMem) BeginWrite(currHeight int64) {
	tree.mtx.Lock()
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	tree.isWriting = true
	binary.BigEndian.PutUint64(tree.currHeight[:], uint64(currHeight))
	tree.currHeightI64 = currHeight
	tree.recentCache.AllocateIfNotExist(currHeight)
	if currHeight > RecentBlockCount {
		tree.recentCache.Prune(currHeight-RecentBlockCount)
	}
}

// End the write phase of block execution
func (tree *NVTreeMem) EndWrite() {
	if !tree.isWriting {
		panic("tree.isWriting cannot be false! bug here...")
	}
	tree.isWriting = false
	tree.mtx.Unlock()
}

// Update or insert a key-position record to B-Tree and RocksDB
// Write the historical record to RocksDB
func (tree *NVTreeMem) Set(k []byte, v int64) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	key := binary.BigEndian.Uint64(k)
	oldV, foundOld := tree.bt.PutNewAndGetOld(key, v)
	if !foundOld {
		oldV = math.MaxInt64
	}
	tree.recentCache.SetAtHeight(tree.currHeightI64, key, oldV)

	if tree.rocksdb == nil || tree.duringInit {
		return
	}
	newK := make([]byte, 0, 1+len(k)+8)
	newK = append(newK, byte(0)) //first byte is always zero
	newK = append(newK, k...)
	newK = append(newK, tree.currHeight[:]...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	tree.batchSet(newK, buf[:])
}

func (tree *NVTreeMem) batchSet(key, value []byte) {
	tree.rocksdb.LockBatch()
	tree.rocksdb.CurrBatch().Set(key, value)
	tree.rocksdb.UnlockBatch()
}

//func (tree *NVTreeMem) batchDelete(key []byte) {
//	tree.rocksdb.LockBatch()
//	tree.rocksdb.CurrBatch().Delete(key)
//	tree.rocksdb.UnlockBatch()
//}

// Get the up-to-date position of k, from the B-Tree
func (tree *NVTreeMem) Get(k []byte) (int64, bool) {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	tree.mtx.RLock()
	defer tree.mtx.RUnlock()
	return tree.bt.Get(binary.BigEndian.Uint64(k))
}

// Get the position of k, at the specified height.
func (tree *NVTreeMem) GetAtHeight(k []byte, height uint64) (position int64, ok bool) {
	if height + RecentBlockCount > uint64(tree.currHeightI64) {
		position, ok = tree.recentCache.FindFrom(int64(height+1), tree.currHeightI64, binary.BigEndian.Uint64(k))
		if ok && position == math.MaxInt64 { // has no old value at height
			return 0, false
		}
		if !ok { // not touched after height
			position, ok = tree.Get(k)
		}
		return
	}
	if h, enable := tree.rocksdb.GetPruneHeight(); enable && height <= h {
		return 0, false
	}
	newK := make([]byte, 1+len(k)+8) // all bytes equal zero
	copy(newK[1:], k)
	binary.BigEndian.PutUint64(newK[1+len(k):], height+1)
	iter := tree.rocksdb.ReverseIterator([]byte{}, newK)
	defer iter.Close()
	if !iter.Valid() || !bytes.Equal(iter.Key()[1:1+len(k)], k) { //not exists or to a different key
		return 0, false
	}

	v := iter.Value()
	if len(v) == 0 {
		ok = false //was deleted
	} else {
		ok = true
		position = int64(binary.BigEndian.Uint64(v))
	}
	return
}

// Delete a key-position record in B-Tree and RocksDB
// Write the historical record to RocksDB
func (tree *NVTreeMem) Delete(k []byte) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	key := binary.BigEndian.Uint64(k)
	oldV, ok := tree.bt.Get(key)
	if !ok {
		panic(fmt.Sprintf("deleting a nonexistent key: %#v bug here...", key))
	}
	tree.bt.Delete(key)
	tree.recentCache.SetAtHeight(tree.currHeightI64, key, oldV)

	if tree.rocksdb == nil || tree.duringInit {
		return
	}
	newK := make([]byte, 0, 1+len(k)+8)
	newK = append(newK, byte(0)) //first byte is always zero
	newK = append(newK, k...)
	newK = append(newK, tree.currHeight[:]...)
	tree.batchSet(newK, []byte{})
}

// Create a forward iterator from the B-Tree
func (tree *NVTreeMem) Iterator(start, end []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	tree.mtx.RLock()
	iter := &ForwardIterMem{
		tree:  tree,
		start: binary.BigEndian.Uint64(start),
		end:   binary.BigEndian.Uint64(end),
	}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = tree.bt.Seek(iter.start)
	iter.Next() //fill key, value, err
	return iter
}

// Create a backward iterator from the B-Tree
func (tree *NVTreeMem) ReverseIterator(start, end []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	tree.mtx.RLock()
	iter := &BackwardIterMem{
		tree:  tree,
		start: binary.BigEndian.Uint64(start),
		end:   binary.BigEndian.Uint64(end),
	}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	var ok bool
	iter.enumerator, ok = tree.bt.Seek(iter.end)
	if ok { // [start, end) end is exclusive
		_, _, _ = iter.enumerator.Prev()
	}
	iter.Next() //fill key, value, err
	return iter
}

func u64ToBz(u64 uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u64)
	return buf[:]
}
