package fuzz

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
var AllZeros = []byte{0, 0, 0, 0, 0, 0, 0, 0}

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

func (tree *NVTreeRef) Close() {
	tree.rocksdb.Close()
}

func (tree *NVTreeRef) BeginWrite(currHeight int64) {
	//fmt.Printf("========= currHeight %d =========\n", currHeight)
	tree.batch = tree.rocksdb.NewBatch()
	binary.BigEndian.PutUint64(tree.currHeight[:], uint64(currHeight))
}

func (tree *NVTreeRef) EndWrite() {
	tree.batch.WriteSync()
	tree.batch.Close()
	tree.batch = nil
}

func (tree *NVTreeRef) Set(k []byte, v int64) {
	copyK := append([]byte{0}, k...) // 0 for historical value
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	tree.batch.Set(append(copyK, tree.currHeight[:]...), buf[:])
	tree.batch.Set(append([]byte{1}, k...), buf[:]) // 1 for latest value
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
	binary.BigEndian.PutUint64(copyK[len(copyK)-8:], height+1) //overwrite the 'AllOnes' part
	iter := tree.rocksdb.ReverseIterator(AllZeros, copyK)
	defer iter.Close()
	//fmt.Printf(" the key : %#v copyK %#v\n", iter.Key(), copyK)
	if !iter.Valid() || len(iter.Value()) == 0 || !bytes.Equal(iter.Key()[1:9], k) {
		return 0, false
	}
	//fmt.Printf(" %#v is changed to %#v at height %v\n", k, iter.Value(), binary.BigEndian.Uint64(key[len(key)-8:]))
	return int64(binary.BigEndian.Uint64(iter.Value())), true
}

func (tree *NVTreeRef) Delete(k []byte) {
	copyK := append([]byte{0}, k...)
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
	ReloadEveryN int
}

var DefaultConfig = FuzzConfig{
	HeightStripe: 100,
	DelCount:     100,
	InitCount:    200,
	QueryCount:   300,
	IterCount:    100,
	IterDistance: 20,
	ReloadEveryN: 100,
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
	err = trMem.Init(nil)
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
			fmt.Printf("====== Now Round %d Height %d ========\n", i, h)
		}
		if i%cfg.ReloadEveryN == 0 && i != 0 {
			fmt.Printf("Reopen begin \n")
			trMem.Close()
			rocksdb.Close()
			rocksdb, err = it.NewRocksDB("idxtree", ".")
			if err != nil {
				panic(err)
			}
			trMem = it.NewNVTreeMem(rocksdb)
			err = trMem.Init(nil)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Reopen end \n")
		}
		changeMap := make(map[string]int64)
		if h != 0 {
			FuzzDelete(trMem, refTree, cfg, rs, changeMap)
		}
		FuzzInit(rocksdb, trMem, refTree, cfg, rs, h, changeMap)
		FuzzQuery(trMem, refTree, cfg, rs, h)
		FuzzIter(trMem, refTree, cfg, rs)
		h += (1 + rs.GetUint64()%uint64(cfg.HeightStripe))
	}
	trMem.Close()
	rocksdb.Close()
	refTree.Close()
	os.RemoveAll("./idxtree.db")
	os.RemoveAll("./idxtreeref.db")
}

func getRandKey(rs randsrc.RandSrc) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], rs.GetUint64())
	return buf[:]
}

func FuzzDelete(trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc, changeMap map[string]int64) {
	for i := 0; i < cfg.DelCount; i++ {
		key := getRandKey(rs)
		//fmt.Printf("Here we get rand key %#v\n", key)
		iter := refTree.Iterator(key, AllOnes)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		key = iter.Key()
		//fmt.Printf("Here we delete %#v\n", key[1:])
		iter.Close()
		changeMap[string(key[1:])] = -1
	}
	return
}

func FuzzInit(rocksdb *it.RocksDB, trMem *it.NVTreeMem, refTree *NVTreeRef, cfg FuzzConfig, rs randsrc.RandSrc, h uint64, changeMap map[string]int64) {
	for i := 0; i < cfg.InitCount; i++ {
		// set new key/value
		key, value := getRandKey(rs), (rs.GetInt64() & ((int64(1) << 48) - 1))
		changeMap[string(key)] = value
	}
	for i := 0; i < cfg.InitCount; i++ {
		// overwrite existing key
		key, value := getRandKey(rs), (rs.GetInt64() & ((int64(1) << 48) - 1))
		iter := refTree.Iterator(key, AllOnes)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		key = iter.Key()
		iter.Close()
		_, ok := refTree.Get(key[1:])
		assert(ok, "Get must return ok")
		changeMap[string(key[1:])] = value
	}
	rocksdb.OpenNewBatch()
	trMem.BeginWrite(int64(h))
	refTree.BeginWrite(int64(h))
	for key, value := range changeMap {
		//fmt.Printf("Set %#v %x\n", []byte(key), value)
		if value < 0 {
			trMem.Delete([]byte(key))
			refTree.Delete([]byte(key))
		} else {
			trMem.Set([]byte(key), value)
			refTree.Set([]byte(key), value)
		}
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

		vMem, okMem = trMem.GetAtHeight(key, h)
		vFuzz, okFuzz = refTree.GetAtHeight(key, h)
		assert(okMem == okFuzz, "ok should be equal")
		assert(vMem == vFuzz, "Value at height should be equal")

		iter := refTree.Iterator(key, AllOnes)
		if !iter.Valid() {
			iter.Close()
			continue
		}
		key = iter.Key()
		v, ok := refTree.Get(key[1:])
		//fmt.Printf("Here we get query key %#v\n", key)
		assert(ok, "Get returns ok")
		assert(binary.BigEndian.Uint64(iter.Value()) == uint64(v), "Value should be equal")
		iter.Close()

		if h == 0 {
			continue
		}
		height := rs.GetUint64() % h
		vMem, okMem = trMem.GetAtHeight(key[1:], height)
		vFuzz, okFuzz = refTree.GetAtHeight(key[1:], height)
		assert(okMem == okFuzz, "ok should be equal")
		if vMem != vFuzz {
			fmt.Printf("AtHeight key %#v height %v okMem %v okFuzz %v vMem %x vFuzz %x\n",
				key[1:], height, okMem, okFuzz, vMem, vFuzz)
		}
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
			if !bytes.Equal(iterMem.Key(), iterFuzz.Key()[1:]) {
				fmt.Printf("m:%#v f:%#v\n", iterMem.Key(), iterFuzz.Key())
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
	for i := 0; i < cfg.IterCount; i++ {
		start, end := getRandKey(rs), getRandKey(rs)
		if rs.GetUint32()%10 == 0 { // 10% possibility
			it := refTree.ReverseIterator(start, end)
			if it.Valid() {
				end = it.Key()[1:] // Change end to exact-match
			}
			it.Close()
		}
		iterMem := trMem.ReverseIterator(start, end)
		iterFuzz := refTree.ReverseIterator(start, end)
		for j := 0; j < cfg.IterDistance; j++ {
			assert(iterMem.Valid() == iterFuzz.Valid(), "valid should be equal")
			if !iterMem.Valid() {
				break
			}
			//fmt.Printf("Iter %d imp %#v ref %#v\n", j, iterMem.Key(), iterFuzz.Key()[1:])
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
