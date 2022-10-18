package indextree

import (
	"encoding/binary"
	"os"
	"testing"

	"fmt"

	"github.com/stretchr/testify/assert"
)


func toBz(k uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], k)
	return buf[:]
}

func mustGet(tree *NVTreeMem, key uint64) int64 {
	//
	fmt.Println("***************TESTING KEY: ", key)
	res, ok := tree.Get(toBz(key)) 
	if !ok {
		panic("Failed to get")
	}

	return res
}

func mustGetH(tree *NVTreeMem, key uint64, height uint64) int64 {
	res, ok := tree.GetAtHeight(toBz(key), height)
	if !ok {
		panic("Failed to get")
	}
	
	return res
}

func createNVTreeMem(dirname string) (*RocksDB, *NVTreeMem) {
	rocksdb, err := NewRocksDB("idxtree", dirname)
	if err != nil {
		panic(err)
	}
	return rocksdb, NewNVTreeMem(rocksdb)
}

func Test1(t *testing.T) {
	rocksdb, tree := createNVTreeMem("./")
	// covers SetDuringInit
	tree.SetDuringInit(true)
	// covers Set when duringInit which just returns
	// tree.Set(toBz(0xABcd1000), 10)
	err := tree.Init(func([]byte) {})
	// covers SetDuringInit
	tree.SetDuringInit(false)

	assert.Equal(t, nil, err)
	tree.BeginWrite(0)
	rocksdb.OpenNewBatch()
	tree.Set(toBz(0xABcd1234), 1)
	tree.Set(toBz(0xABcd1235), 2)
	tree.Set(toBz(0x0bcd1234), 0)
	tree.Set(toBz(0xABed1234), 3)
	tree.Set(toBz(0xBBed1234), 3)
	tree.Set(toBz(0xFBed1234), 4)

	// covers ActiveCount
	assert.Equal(t, 6, tree.ActiveCount())
	tree.Delete(toBz(0xBBed1234))

	// covers ActiveCount
	assert.Equal(t, 5, tree.ActiveCount())
	// covers RecentCaches' DidNotTouchInRange
	assert.Equal(t, false, tree.recentCache.DidNotTouchInRange(0, 1, 0xABcd1234))
	assert.Equal(t, false, tree.recentCache.DidNotTouchInRange(0, 1, 0xABcd1235))
	assert.Equal(t, true, tree.recentCache.DidNotTouchInRange(0, 1, 0xABcd1236))

	rocksdb.CloseOldBatch()
	tree.EndWrite()
	tree.Close()
	rocksdb.Close()

	rocksdb, tree = createNVTreeMem("./")

	// covers SetDuringInit
	tree.SetDuringInit(true)
	err = tree.Init(func(k []byte) {})
	// covers SetDuringInit
	tree.SetDuringInit(false)
	assert.Equal(t, nil, err)

	assert.Equal(t, int64(0), mustGet(tree, 0x0bcd1234))
	assert.Equal(t, int64(1), mustGet(tree, 0xABcd1234))
	assert.Equal(t, int64(2), mustGet(tree, 0xABcd1235))
	assert.Equal(t, int64(3), mustGet(tree, 0xABed1234))
	assert.Equal(t, int64(4), mustGet(tree, 0xFBed1234))

	assert.Equal(t, int64(0), mustGetH(tree, 0x0bcd1234, 1))
	assert.Equal(t, int64(1), mustGetH(tree, 0xABcd1234, 0))
	assert.Equal(t, int64(2), mustGetH(tree, 0xABcd1235, 1))
	assert.Equal(t, int64(3), mustGetH(tree, 0xABed1234, 9))
	assert.Equal(t, int64(4), mustGetH(tree, 0xFBed1234, 10))

	rocksdb.OpenNewBatch()
	tree.BeginWrite(10)
	tree.Set(toBz(0xABcd1234), 111)
	tree.Set(toBz(0x1bcd1234), 100)
	// covers ActiveCount
	assert.Equal(t, 6, tree.ActiveCount())
	rocksdb.CloseOldBatch()
	tree.EndWrite()

	assert.Equal(t, int64(1), mustGetH(tree, 0xABcd1234, 0))
	assert.Equal(t, int64(1), mustGetH(tree, 0xABcd1234, 5))
	assert.Equal(t, int64(111), mustGetH(tree, 0xABcd1234, 10))
	assert.Equal(t, int64(111), mustGetH(tree, 0xABcd1234, 11))
	assert.Equal(t, int64(100), mustGetH(tree, 0x1bcd1234, 10))

	_, ok := tree.Get(toBz(0x1110bcd1234))
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight(toBz(0x1bcd1234), 9)
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight(toBz(0xFFcd1234), 9)
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight(toBz(0x1bcd1234), 8)
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight(toBz(0x1bcd1234FFFFFF), 8)
	assert.Equal(t, false, ok)

	assert.Equal(t, int64(0), mustGet(tree, 0x0bcd1234))
	assert.Equal(t, int64(100), mustGet(tree, 0x1bcd1234))
	assert.Equal(t, int64(111), mustGet(tree, 0xABcd1234))
	assert.Equal(t, int64(2), mustGet(tree, 0xABcd1235))
	assert.Equal(t, int64(3), mustGet(tree, 0xABed1234))
	assert.Equal(t, int64(4), mustGet(tree, 0xFBed1234))

	iter := tree.Iterator(toBz(0xABcd1234), toBz(0x0bcd1234))
	assert.Equal(t, false, iter.Valid())
	iter.Close()
	iter = tree.Iterator(toBz(0x0bcd1234), toBz(0xABcd1234))
	start, end := iter.Domain()
	assert.Equal(t, start, toBz(0x0bcd1234))
	assert.Equal(t, end, toBz(0xABcd1234))
	assert.Equal(t, toBz(0x0bcd1234), iter.Key())
	assert.Equal(t, int64(0), iter.Value())
	assert.Equal(t, true, iter.Valid())
	iter.Next()
	assert.Equal(t, toBz(0x1bcd1234), iter.Key())
	assert.Equal(t, int64(100), iter.Value())
	assert.Equal(t, true, iter.Valid())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()

	reviter := tree.ReverseIterator(toBz(0xFFcd1234), toBz(0xABed1234))
	assert.Equal(t, false, reviter.Valid())
	reviter.Close()
	reviter = tree.ReverseIterator(toBz(0xABed1234), toBz(0xFFcd1234))
	start, end = reviter.Domain()
	assert.Equal(t, start, toBz(0xABed1234))
	assert.Equal(t, end, toBz(0xFFcd1234))
	assert.Equal(t, toBz(0xFBed1234), reviter.Key())
	assert.Equal(t, int64(4), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Next()
	assert.Equal(t, toBz(0xABed1234), reviter.Key())
	assert.Equal(t, int64(3), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Next()
	assert.Equal(t, false, reviter.Valid())
	reviter.Close()

	reviter = tree.ReverseIterator(toBz(0xABed1234), toBz(0xFBed1234))
	assert.Equal(t, toBz(0xABed1234), reviter.Key())
	assert.Equal(t, int64(3), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Close()

	tree.Close()
	rocksdb.Close()

	// NVTreeMem to test more heights beyond RecentBlockCount
	rocksdb, tree = createNVTreeMem("./")

	// covers SetDuringInit
	tree.SetDuringInit(true)
	err = tree.Init(func(k []byte) {})
	// covers SetDuringInit
	tree.SetDuringInit(false)
	assert.Equal(t, nil, err)

	tree.BeginWrite(0)
	rocksdb.OpenNewBatch()
	tree.Set(toBz(0), 1000)
	tree.Set(toBz(1), 2000)
	rocksdb.CloseOldBatch()
	tree.EndWrite()

	tree.BeginWrite(1)
	rocksdb.OpenNewBatch()
	tree.Set(toBz(0xABcd1234), 1)
	tree.Set(toBz(0xABcd1235), 2)
	tree.Set(toBz(0x0bcd1234), 0)
	tree.Set(toBz(0xABed1234), 3)

	rocksdb.CloseOldBatch()
	tree.EndWrite()

	// check that height 0 & 1 is not empty
	assert.NotEmpty(t, tree.recentCache.caches[0])
	assert.NotEmpty(t, tree.recentCache.caches[1])

	// UP TO HERE, CURRHEIGHT IS 1
	// height at 1 + RecentBlockCount (1024)
	// so 1 after 1024th height from height 1
	tree.BeginWrite(1025)
	rocksdb.OpenNewBatch()
	tree.Set(toBz(0xABcd1234), 1)
	tree.Set(toBz(0xABcd1235), 2)
	tree.Set(toBz(0x0bcd1234), 0)
	tree.Set(toBz(0xABed1234), 3)

	rocksdb.CloseOldBatch()
	tree.EndWrite()

	// check that caches do not include height 1 anymore
	// 1025 - RecentBlockCount is 1
	assert.Empty(t, tree.recentCache.caches[1])

	// try getting value from height 0 which is not in cache
	assert.Equal(t, int64(1000), mustGetH(tree, 0, 0))
	assert.Equal(t, int64(2000), mustGetH(tree, 1, 0))

	tree.Close()
	rocksdb.Close()
		
	os.RemoveAll("./idxtree.db")
}
