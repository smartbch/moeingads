package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/coinexchain/randsrc"

	it "github.com/moeing-chain/MoeingADS/indextree"
	"github.com/moeing-chain/MoeingADS/types"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <round-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	roundCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	RunFuzz(roundCount, DefaultConfig, randFilename)
}

var AllOnes = []byte{255,255,255,255, 255,255,255,255}

type NVTreeRef struct {
	rocksdb    *it.RocksDB
	batch      types.Batch
	currHeight [8]byte
}

func (tree *NVTreeRef) Init(dirname string) (err error) {
	tree.rocksdb, err = it.NewRocksDB("idxtreeref", dirname)
	if err != nil {
		return err
	}
	return nil
}

func (tree *NVTreeRef) BeginWrite(currHeight int64) {
	tree.batch = tree.rocksdb.NewBatch()
	binary.BigEndian.PutUint64(tree.currHeight[:], uint64(currHeight))
}

func (tree *NVTreeRef) EndWrite() {
	tree.batch.WriteSync()
	tree.batch.Close()
	tree.batch = nil
}

func (tree *NVTreeRef) Set(k []byte, v int64) {
	copyK := append([]byte{0}, k...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	tree.batch.Set(append(copyK, tree.currHeight[:]...), buf[:])
	tree.batch.Set(append([]byte{1}, k...), buf[:])
}

func (tree *NVTreeRef) Get(k []byte) (int64, bool) {
	value := tree.rocksdb.Get(append([]byte{1}, k...))
	if len(value) == 0 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(value)), true
}

func (tree *NVTreeRef) GetAtHeight(k []byte, height uint64) (position int64, ok bool) {
	copyK := append([]byte{0}, k...)
	copyK = append(copyK, AllOnes...)
	binary.BigEndian.PutUint64(copyK[len(copyK)-8:], height)
	value := tree.rocksdb.Get(copyK)
	if len(value) == 0 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(value)), true
}

func (tree *NVTreeRef) Delete(k []byte) {
	copyK := append([]byte{}, k...)
	tree.batch.Set(append(copyK, tree.currHeight[:]...), []byte{})
	tree.batch.Delete(append([]byte{1}, k...))
}

func (tree *NVTreeRef) Iterator(start, end []byte) types.Iterator {
	return tree.rocksdb.Iterator(append([]byte{1}, start...), append([]byte{1}, end...))
}

func (tree *NVTreeRef) ReverseIterator(start, end []byte) types.Iterator {
	return tree.rocksdb.ReverseIterator(append([]byte{1}, start...), append([]byte{1}, end...))
}

func assert(b bool, s string) {
	if !b {
		panic(s)
	}
}

type FuzzConfig struct {
	HeightStripe int
	InitCount    int
	QueryCount   int
	IterCount    int
	IterDistance int
	DelCount     int
}

var DefaultConfig = FuzzConfig{
	HeightStripe: 100,
	DelCount:     100,
	InitCount:    200,
	QueryCount:   300,
	IterCount:    100,
	IterDistance: 20,
}

func RunFuzz(roundCount int, cfg FuzzConfig, randFilename string) {
	os.RemoveAll("./idxtree.db")
	os.RemoveAll("./idxtreeref.db")
	rs := randsrc.NewRandSrcFromFile(randFilename)
	rocksdb, err := it.NewRocksDB("idxtree", ".")
	if err != nil {
		panic(err)
	}
	trMem := it.NewNVTreeMem(rocksdb)
	err = trMem.Init(func([]byte) {})
	if err != nil {
		panic(err)
	}
	refTree := &NVTreeRef{}
	err = refTree.Init("./")
	if err != nil {
		panic(err)
	}

	h := uint64(0)
	for i := 0; i < roundCount; i++ {
		if i%1000 == 0 {
			fmt.Printf("====== Now Round %d ========\n", i)
		}
		if h == 0 {
			rocksdb.OpenNewBatch()
			trMem.BeginWrite(0)
			refTree.BeginWrite(0)
		} else {
			FuzzDelete(rocksdb, trMem, refTree, cfg, rs, h)
		}
		FuzzInit(rocksdb, trMem, refTree, cfg, rs)
		FuzzQuery(trMem, refTree, cfg, rs, h)
		FuzzIter(trMem, refTree, cfg, rs)
		h += rs.GetUint64()%uint64(cfg.HeightStripe)
	}
	os.RemoveAll("./idxtree.db")
	os.RemoveAll("./idxtreeref.db")
}

func getRandKey(rs randsrc.RandSrc) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], rs.GetUint64())
	return buf[:]
}

func FuzzDelete(rocksdb *it.RocksDB, trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc, h uint64) {
	rocksdb.OpenNewBatch()
	trMem.BeginWrite(int64(h))
	refTree.BeginWrite(int64(h))
	for i := 0; i < cfg.DelCount; i++ {
		key := getRandKey(rs)
		iter := refTree.Iterator(key, nil)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		iter.Close()
		key = iter.Key()
		trMem.Delete(key)
		refTree.Delete(key)
	}
}

func FuzzInit(rocksdb *it.RocksDB, trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.InitCount; i++ {
		// set new key/value
		key, value := getRandKey(rs), (rs.GetInt64()&((int64(1)<<48)-1))
		//fmt.Printf("Add %v %d\n", key, value)
		trMem.Set(key, value)
		refTree.Set(key, value)
	}
	for i := 0; i < cfg.InitCount; i++ {
		// overwrite existing key
		key := getRandKey(rs)
		iter := refTree.Iterator(key, nil)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		key = iter.Key()
		iter.Close()
		value, ok := refTree.Get(key)
		assert(ok, "Get returns ok")
		//fmt.Printf("Add %v %d\n", key, value)
		trMem.Set(key, value)
		refTree.Set(key, value)
	}
	trMem.EndWrite()
	rocksdb.CloseOldBatch()
	refTree.EndWrite()
}

func FuzzQuery(trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc, h uint64) {
	for i := 0; i < cfg.QueryCount; i++ {
		key := getRandKey(rs)
		vMem, okMem := trMem.Get(key)
		vFuzz, okFuzz := refTree.Get(key)
		assert(okMem == okFuzz, "ok should be equal")
		assert(vMem == vFuzz, "Value at height should be equal")

		iter := refTree.Iterator(key, nil)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		key = iter.Key()
		iter.Close()
		v, ok := refTree.Get(key)
		assert(ok, "Get returns ok")
		assert(binary.BigEndian.Uint64(iter.Value()) == uint64(v), "Value should be equal")

		height := rs.GetUint64()%h
		vMem, okMem = trMem.GetAtHeight(key, height)
		vFuzz, okFuzz = refTree.GetAtHeight(key, height)
		assert(okMem == okFuzz, "ok should be equal")
		assert(vMem == vFuzz, "Value at height should be equal")
	}
}

func FuzzIter(trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.IterCount; i++ {
		start, end := getRandKey(rs), getRandKey(rs)
		iterMem := trMem.Iterator(start, end)
		iterFuzz := refTree.Iterator(start, end)
		//fmt.Printf("Iter between %#v %#v\n", start, end)
		for j := 0; j < cfg.IterDistance; j++ {
			assert(iterMem.Valid() == iterFuzz.Valid(), "valid should be equal")
			if !iterMem.Valid() {
				break
			}
			//if !bytes.Equal(iterMem.Key(), iterFuzz.Key()[1:]) {
			//	fmt.Printf("m:%#v f:%#v\n", iterMem.Key(), iterFuzz.Key())
			//}
			assert(bytes.Equal(iterMem.Key(), iterFuzz.Key()[1:]), "key should be equal")
			vFuzz := binary.BigEndian.Uint64(iterFuzz.Value())
			assert(iterMem.Value() == int64(vFuzz), "value should be equal")
			iterMem.Next()
			iterFuzz.Next()
		}
		iterMem.Close()
		iterFuzz.Close()
	}
	for i := 0; i < cfg.IterCount; i++ {
		start, end := getRandKey(rs), getRandKey(rs)
		iterMem := trMem.ReverseIterator(start, end)
		iterFuzz := refTree.ReverseIterator(start, end)
		for j := 0; j < cfg.IterDistance; j++ {
			assert(iterMem.Valid() == iterFuzz.Valid(), "valid should be equal")
			if !iterMem.Valid() {
				break
			}
			assert(bytes.Equal(iterMem.Key(), iterFuzz.Key()[1:]), "key should be equal")
			vFuzz := binary.BigEndian.Uint64(iterFuzz.Value())
			assert(iterMem.Value() == int64(vFuzz), "value should be equal")
			iterMem.Next()
			iterFuzz.Next()
		}
		iterMem.Close()
		iterFuzz.Close()
	}
}

