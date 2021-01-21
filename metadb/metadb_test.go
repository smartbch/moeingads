package metadb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moeing-chain/MoeingADS/datatree"
	"github.com/moeing-chain/MoeingADS/indextree"
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
	assert.Equal(t, int64(0), mdb.GetLastPrunedTwig())
	assert.Equal(t, int64(0), mdb.GetMaxSerialNum())
	//assert.Equal(t, int64(0), mdb.GetActiveEntryCount())
	assert.Equal(t, int64(0), mdb.GetOldestActiveTwigID())

	mdb.SetCurrHeight(100)
	mdb.SetLastPrunedTwig(2)
	mdb.IncrMaxSerialNum()
	//mdb.IncrActiveEntryCount()
	//mdb.IncrActiveEntryCount()
	//mdb.DecrActiveEntryCount()
	mdb.IncrOldestActiveTwigID()

	assert.Equal(t, int64(100), mdb.GetCurrHeight())
	assert.Equal(t, int64(2), mdb.GetLastPrunedTwig())
	assert.Equal(t, int64(1), mdb.GetMaxSerialNum())
	//assert.Equal(t, int64(1), mdb.GetActiveEntryCount())
	assert.Equal(t, int64(1), mdb.GetOldestActiveTwigID())

	assert.Equal(t, int64(0), mdb.GetTwigMtFileSize())
	assert.Equal(t, int64(0), mdb.GetEntryFileSize())

	mdb.SetTwigMtFileSize(1000)
	mdb.SetEntryFileSize(2000)
	mdb.setTwigHeight(1, 100)
	mdb.setTwigHeight(2, 120)
	mdb.SetEdgeNodes([]byte("edge nodes data"))
	mdb.SetRootHash(rootHash)

	mdb.Commit()
	kvdb.CloseOldBatch()
	kvdb.OpenNewBatch()

	assert.Equal(t, int64(1000), mdb.GetTwigMtFileSize())
	assert.Equal(t, int64(2000), mdb.GetEntryFileSize())
	assert.Equal(t, int64(120), mdb.GetTwigHeight(2))
	assert.Equal(t, int64(100), mdb.GetTwigHeight(1))
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(3))
	assert.Equal(t, []byte("edge nodes data"), mdb.GetEdgeNodes())
	assert.Equal(t, rootHash, mdb.GetRootHash())

	mdb.maxSerialNum = 5*datatree.LeafCountInTwig - 1
	mdb.SetCurrHeight(150)
	mdb.IncrMaxSerialNum()
	mdb.DeleteTwigHeight(2)

	mdb.Commit()
	kvdb.CloseOldBatch()

	assert.Equal(t, int64(-1), mdb.GetTwigHeight(2))
	assert.Equal(t, int64(150), mdb.GetTwigHeight(5))

	kvdb.Close()
	mdb.Close()

	kvdb, err = indextree.NewRocksDB("test", ".")
	assert.Equal(t, nil, err)
	kvdb.OpenNewBatch()
	mdb = NewMetaDB(kvdb)
	mdb.ReloadFromKVDB()

	assert.Equal(t, int64(150), mdb.GetCurrHeight())
	assert.Equal(t, int64(2), mdb.GetLastPrunedTwig())
	assert.Equal(t, int64(5*datatree.LeafCountInTwig), mdb.GetMaxSerialNum())
	//assert.Equal(t, int64(1), mdb.GetActiveEntryCount())
	assert.Equal(t, int64(1), mdb.GetOldestActiveTwigID())
	assert.Equal(t, int64(1000), mdb.GetTwigMtFileSize())
	assert.Equal(t, int64(2000), mdb.GetEntryFileSize())
	assert.Equal(t, int64(100), mdb.GetTwigHeight(1))
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(3))
	assert.Equal(t, []byte("edge nodes data"), mdb.GetEdgeNodes())
	assert.Equal(t, int64(-1), mdb.GetTwigHeight(2))
	assert.Equal(t, int64(150), mdb.GetTwigHeight(5))

	mdb.Close()
	kvdb.Close()
	err = os.RemoveAll("./test.db")
	assert.Equal(t, nil, err)

}
