package fuzzhis

// Fuzz test for historical state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/coinexchain/randsrc"

	"github.com/smartbch/moeingads"
	it "github.com/smartbch/moeingads/indextree"
	"github.com/smartbch/moeingads/store"
	"github.com/smartbch/moeingads/store/rabbit"
	"github.com/smartbch/moeingads/types"
)

type FuzzConfig struct {
	RabbitCount  int
	MaxNumOfKeys int
	InitCount    int
	HeightStride int
	SetCount     int
	DelCount     int
	QueryCount   int
	ReloadEveryN int
}

var DefaultConfig = FuzzConfig{
	RabbitCount:  10,
	MaxNumOfKeys: 10000,
	InitCount:    2000,
	HeightStride: 100,
	SetCount:     200,
	DelCount:     100,
	QueryCount:   300,
	ReloadEveryN: 100,
}

func runTest(cfg FuzzConfig) {
	randFilename := os.Getenv("RANDFILE")
	if len(randFilename) == 0 {
		fmt.Printf("No RANDFILE specified. Exiting...")
		return
	}
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		panic(err)
	}

	RunFuzz(roundCount, cfg, randFilename)
}

var AllOnes = []byte{255, 255, 255, 255, 255, 255, 255, 255}

type RefHisDb struct {
	rocksdb    *it.RocksDB
	batch      types.Batch
	currHeight [8]byte
}

func NewRefHisDb(dirname string) *RefHisDb {
	db := &RefHisDb{}
	var err error
	db.rocksdb, err = it.NewRocksDB(dirname, ".")
	if err != nil {
		panic(err)
	}
	return db
}

func (db *RefHisDb) Close() {
	db.rocksdb.Close()
}

func (db *RefHisDb) BeginWrite(currHeight int64) {
	//fmt.Printf("========= currHeight %d =========\n", currHeight)
	db.batch = db.rocksdb.NewBatch()
	binary.BigEndian.PutUint64(db.currHeight[:], uint64(currHeight))
}

func (db *RefHisDb) EndWrite() {
	db.batch.WriteSync()
	db.batch.Close()
	db.batch = nil
}

func (db *RefHisDb) Set(k, v []byte) {
	copyK := append([]byte{0}, k...) // 0 for historical value
	db.batch.Set(append(copyK, db.currHeight[:]...), v)
	db.batch.Set(append([]byte{1}, k...), v) // 1 for latest value
}

func (db *RefHisDb) Get(k []byte) []byte {
	return db.rocksdb.Get(append([]byte{1}, k...))
}

func (db *RefHisDb) GetAtHeight(k []byte, height uint64) []byte {
	copyK := append([]byte{0}, k...)
	copyK = append(copyK, AllOnes...)
	binary.BigEndian.PutUint64(copyK[len(copyK)-8:], height+1) //overwrite the 'AllOnes' part
	var allZeros [1 + 32 + 8]byte
	iter := db.rocksdb.ReverseIterator(allZeros[:], copyK)
	defer iter.Close()
	//fmt.Printf(" the key : %#v copyK %#v\n", iter.Key(), copyK)
	if !iter.Valid() || len(iter.Value()) == 0 || !bytes.Equal(iter.Key()[1:1+len(k)], k) {
		return nil
	}
	return iter.Value()
}

func (db *RefHisDb) Delete(k []byte) {
	copyK := append([]byte{0}, k...)
	db.batch.Set(append(copyK, db.currHeight[:]...), []byte{})
	db.batch.Delete(append([]byte{1}, k...))
}

func assert(b bool, s string) {
	if !b {
		panic(s)
	}
}

func NewRoot(dir string) *store.RootStore {
	var start [8]byte
	var end [8]byte
	for i := 0; i < 8; i++ {
		end[i] = ^start[i]
	}
	mads, err := moeingads.NewMoeingADS(dir, true, [][]byte{start[:], end[:]})
	if err != nil {
		panic(err)
	}
	return store.NewRootStore(mads, func(k []byte) bool { return false })
}

func getRandKey(numOfKeys int, rs randsrc.RandSrc) []byte {
	var buf [8]byte
	u64 := rs.GetUint64() % uint64(numOfKeys)
	binary.LittleEndian.PutUint64(buf[:], u64)
	hash := sha256.Sum256(buf[:])
	return hash[:]
}

func RunFuzz(roundCount int, cfg FuzzConfig, randFilename string) {
	os.RemoveAll("./RefHisDb.db")
	os.RemoveAll("./MoeingADS.dir")
	rs := randsrc.NewRandSrcFromFile(randFilename)
	ref := NewRefHisDb("./RefHisDb")
	root := NewRoot("./MoeingADS.dir")

	h := uint64(0)
	fuzzChange(cfg.MaxNumOfKeys, cfg.InitCount, 0, ref, root, rs, h)
	for i := 0; i < roundCount; i++ {
		h += (1 + rs.GetUint64()%uint64(cfg.HeightStride))
		if i%10 == 0 {
			fmt.Printf("====== Now Round %d Height %d ========\n", i, h)
		}
		if i%cfg.ReloadEveryN == 0 && i != 0 {
			fmt.Printf("Reopen begin \n")
			ref.Close()
			root.Close()
			ref = NewRefHisDb("./RefHisDb")
			root = NewRoot("./MoeingADS.dir")
			fmt.Printf("Reopen end \n")
		}
		//fmt.Printf("fuzzChange: \n")
		fuzzChange(cfg.MaxNumOfKeys, cfg.SetCount, cfg.DelCount, ref, root, rs, h)
		//fmt.Printf("fuzzQuery: \n")
		fuzzQuery(cfg, ref, root, rs, h)
	}
	ref.Close()
	root.Close()
	os.RemoveAll("./RefHisDb.db")
	os.RemoveAll("./MoeingADS.dir")
}

func fuzzChange(numOfKeys, setCount, delCount int, ref *RefHisDb, root *store.RootStore, rs randsrc.RandSrc, h uint64) {
	ref.BeginWrite(int64(h))
	root.SetHeight(int64(h))
	trunk := root.GetTrunkStore(1000).(*store.TrunkStore)
	rbt := rabbit.NewRabbitStore(trunk)

	for i := 0; i < setCount; i++ {
		key := getRandKey(numOfKeys, rs)
		value := rs.GetBytes(32)
		ref.Set(key, value)
		rbt.Set(key, value)
		//fmt.Printf("SET H %d key %#v %#v\n", h, key, value)
	}
	for i := 0; i < delCount; i++ {
		key := getRandKey(numOfKeys, rs)
		ref.Delete(key)
		rbt.Delete(key)
		//fmt.Printf("DEL H %d key %#v\n", h, key)
	}
	ref.EndWrite()
	rbt.CloseAndWriteBack(true)
	trunk.Close(true)
}

func fuzzQuery(cfg FuzzConfig, ref *RefHisDb, root *store.RootStore, rs randsrc.RandSrc, height uint64) {
	for i := 0; i < cfg.RabbitCount; i++ {
		h := rs.GetUint64() % height
		rbt := rabbit.NewReadOnlyRabbitStoreAtHeight(root, h)
		for j := 0; j < cfg.QueryCount; j++ {
			key := getRandKey(cfg.MaxNumOfKeys, rs)
			vRef := ref.GetAtHeight(key, h)
			vImp := rbt.Get(key)
			if !bytes.Equal(vRef, vImp) {
				fmt.Printf("h %d key %#v vRef %#v vImp %#v\n", h, key, vRef, vImp)
			}
			assert(bytes.Equal(vRef, vImp), "Mismatch!")
		}
		rbt.Close()
	}
}
