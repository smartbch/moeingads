package indextree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustGet(tree *NVTreeMem, key []byte) uint64 {
	res, ok := tree.Get(key)
	if !ok {
		panic("Failed to get")
	}
	return res
}

func mustGetH(tree *NVTreeMem, key []byte, height uint64) uint64 {
	res, ok := tree.GetAtHeight(key, height)
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
	err := tree.Init(func([]byte) {})
	assert.Equal(t, nil, err)
	tree.BeginWrite(0)
	rocksdb.OpenNewBatch()
	tree.Set([]byte("ABcd1234"), 1)
	tree.Set([]byte("ABcd1235"), 2)
	tree.Set([]byte("0bcd1234"), 0)
	tree.Set([]byte("ABxd1234"), 3)
	tree.Set([]byte("BBxd1234"), 3)
	tree.Set([]byte("ZBxd1234"), 4)
	tree.Delete([]byte("BBxd1234"))
	rocksdb.CloseOldBatch()
	tree.EndWrite()
	tree.Close()
	rocksdb.Close()

	rocksdb, tree = createNVTreeMem("./")
	err = tree.Init(func(k []byte) {})
	assert.Equal(t, nil, err)


	assert.Equal(t, uint64(0), mustGet(tree, []byte("0bcd1234")))
	assert.Equal(t, uint64(1), mustGet(tree, []byte("ABcd1234")))
	assert.Equal(t, uint64(2), mustGet(tree, []byte("ABcd1235")))
	assert.Equal(t, uint64(3), mustGet(tree, []byte("ABxd1234")))
	assert.Equal(t, uint64(4), mustGet(tree, []byte("ZBxd1234")))

	assert.Equal(t, uint64(0), mustGetH(tree, []byte("0bcd1234"), 1))
	assert.Equal(t, uint64(1), mustGetH(tree, []byte("ABcd1234"), 0))
	assert.Equal(t, uint64(2), mustGetH(tree, []byte("ABcd1235"), 1))
	assert.Equal(t, uint64(3), mustGetH(tree, []byte("ABxd1234"), 9))
	assert.Equal(t, uint64(4), mustGetH(tree, []byte("ZBxd1234"), 10))

	rocksdb.OpenNewBatch()
	tree.BeginWrite(10)
	tree.Set([]byte("ABcd1234"), 111)
	tree.Set([]byte("1bcd1234"), 100)
	rocksdb.CloseOldBatch()
	tree.EndWrite()

	assert.Equal(t, uint64(1), mustGetH(tree, []byte("ABcd1234"), 0))
	assert.Equal(t, uint64(1), mustGetH(tree, []byte("ABcd1234"), 5))
	assert.Equal(t, uint64(111), mustGetH(tree, []byte("ABcd1234"), 10))
	assert.Equal(t, uint64(111), mustGetH(tree, []byte("ABcd1234"), 11))
	assert.Equal(t, uint64(100), mustGetH(tree, []byte("1bcd1234"), 10))

	_, ok := tree.Get([]byte("---0bcd1234"))
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight([]byte("1bcd1234"), 9)
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight([]byte("ZZcd1234"), 9)
	assert.Equal(t, false, ok)
	_, ok = tree.GetAtHeight([]byte("1bcd1234"), 8)
	assert.Equal(t, false, ok)
	newK := append([]byte("1bcd1234"), []byte{255,255,255,255,255,255,255,255,255}...)
	_, ok = tree.GetAtHeight(newK, 8)
	assert.Equal(t, false, ok)

	assert.Equal(t, uint64(0), mustGet(tree, []byte("0bcd1234")))
	assert.Equal(t, uint64(100), mustGet(tree, []byte("1bcd1234")))
	assert.Equal(t, uint64(111), mustGet(tree, []byte("ABcd1234")))
	assert.Equal(t, uint64(2), mustGet(tree, []byte("ABcd1235")))
	assert.Equal(t, uint64(3), mustGet(tree, []byte("ABxd1234")))
	assert.Equal(t, uint64(4), mustGet(tree, []byte("ZBxd1234")))

	iter := tree.Iterator([]byte("ABcd1234"), []byte("0bcd1234"))
	assert.Equal(t, false, iter.Valid())
	iter.Close()
	iter = tree.Iterator([]byte("0bcd1234"), []byte("ABcd1234"))
	start, end := iter.Domain()
	assert.Equal(t, start, []byte("0bcd1234"))
	assert.Equal(t, end, []byte("ABcd1234"))
	assert.Equal(t, []byte("0bcd1234"), iter.Key())
	assert.Equal(t, uint64(0), iter.Value())
	assert.Equal(t, true, iter.Valid())
	iter.Next()
	assert.Equal(t, []byte("1bcd1234"), iter.Key())
	assert.Equal(t, uint64(100), iter.Value())
	assert.Equal(t, true, iter.Valid())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()

	reviter := tree.ReverseIterator([]byte("ZZcd1234"), []byte("ABxd1234"))
	assert.Equal(t, false, reviter.Valid())
	reviter.Close()
	reviter = tree.ReverseIterator([]byte("ABxd1234"), []byte("ZZcd1234"))
	start, end = reviter.Domain()
	assert.Equal(t, start, []byte("ABxd1234"))
	assert.Equal(t, end, []byte("ZZcd1234"))
	assert.Equal(t, []byte("ZBxd1234"), reviter.Key())
	assert.Equal(t, uint64(4), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Next()
	assert.Equal(t, []byte("ABxd1234"), reviter.Key())
	assert.Equal(t, uint64(3), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Next()
	assert.Equal(t, false, reviter.Valid())
	reviter.Close()

	reviter = tree.ReverseIterator([]byte("ABxd1234"), []byte("ZBxd1234"))
	assert.Equal(t, []byte("ABxd1234"), reviter.Key())
	assert.Equal(t, uint64(3), reviter.Value())
	assert.Equal(t, true, reviter.Valid())
	reviter.Close()

	tree.Close()
	rocksdb.Close()

	os.RemoveAll("./idxtree.db")
}
