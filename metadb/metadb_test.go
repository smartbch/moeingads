package metadb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/indextree"
)

func TestMetaDB(t *testing.T) {
	var rootHash [32]byte
	for i := range rootHash {
		rootHash[i] = byte(i)
	}
	err := os.RemoveAll("./test.db")
	assert.Equal(t, nil, err)

	kvdb, err := indextree.NewRocksDB("test", ".")
	assert.Equal(t, nil, err)
	kvdb.OpenNewBatch()
	mdb := NewMetaDB(kvdb)
	mdb.ReloadFromKVDB()

	assert.Equal(t, int64(0), mdb.GetCurrHeight())
	assert.Equal(t, int64(0), mdb.GetLastPrunedTwig(0))
	assert.Equal(t, int64(0), mdb.GetMaxSerialNum(0))
	assert.Equal(t, int64(0), mdb.GetOldestActiveTwigID(0))

	mdb.SetCurrHeight(100)
	mdb.SetLastPrunedTwig(0, 2)
	mdb.IncrMaxSerialNum(0)
	mdb.IncrOldestActiveTwigID(0)

	assert.Equal(t, int64(100), mdb.GetCurrHeight())
	assert.Equal(t, int64(2), mdb.GetLastPrunedTwig(0))
	assert.Equal(t, int64(1), mdb.GetMaxSerialNum(0))
	assert.Equal(t, int64(1), mdb.GetOldestActiveTwigID(0))

	assert.Equal(t, int64(0), mdb.GetTwigMtFileSize(0))
	assert.Equal(t, int64(0), mdb.GetEntryFileSize(0))

	mdb.SetTwigMtFileSize(0, 1000)
	mdb.SetEntryFileSize(0, 2000)
	mdb.setTwigHeight(0, 1, 100)
	mdb.setTwigHeight(0, 2, 120)
	mdb.SetEdgeNodes(0, []byte("edge nodes data"))
	mdb.SetRootHash(0, rootHash)

	mdb.Commit()
	kvdb.CloseOldBatch()
	kvdb.OpenNewBatch()

	assert.Equal(t, int64(1000), mdb.GetTwigMtFileSize(0))
	assert.Equal(t, int64(2000), mdb.GetEntryFileSize(0))
	assert.Equal(t, int64(120), mdb.GetTwigHeight(0, 2))
	assert.Equal(t, int64(100), mdb.GetTwigHeight(0, 1))
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(0, 3))
	assert.Equal(t, []byte("edge nodes data"), mdb.GetEdgeNodes(0))
	assert.Equal(t, rootHash, mdb.GetRootHash(0))

	mdb.maxSerialNum[0] = 5*datatree.LeafCountInTwig - 1
	mdb.SetCurrHeight(150)
	mdb.IncrMaxSerialNum(0)
	mdb.DeleteTwigHeight(0, 2)

	mdb.Commit()
	kvdb.CloseOldBatch()

	assert.Equal(t, int64(-1), mdb.GetTwigHeight(0, 2))
	assert.Equal(t, int64(150), mdb.GetTwigHeight(0, 5))

	kvdb.Close()
	mdb.Close()

	kvdb, err = indextree.NewRocksDB("test", ".")
	assert.Equal(t, nil, err)
	kvdb.OpenNewBatch()
	mdb = NewMetaDB(kvdb)
	mdb.ReloadFromKVDB()

	assert.Equal(t, int64(150), mdb.GetCurrHeight())
	assert.Equal(t, int64(2), mdb.GetLastPrunedTwig(0))
	assert.Equal(t, int64(5*datatree.LeafCountInTwig), mdb.GetMaxSerialNum(0))
	assert.Equal(t, int64(1), mdb.GetOldestActiveTwigID(0))
	assert.Equal(t, int64(1000), mdb.GetTwigMtFileSize(0))
	assert.Equal(t, int64(2000), mdb.GetEntryFileSize(0))
	assert.Equal(t, int64(100), mdb.GetTwigHeight(0, 1))
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(0, 3))
	assert.Equal(t, []byte("edge nodes data"), mdb.GetEdgeNodes(0))
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(0, 2))
	assert.Equal(t, int64(150), mdb.GetTwigHeight(0, 5))

	mdb.Close()
	kvdb.Close()
	err = os.RemoveAll("./test.db")
	assert.Equal(t, nil, err)
}
