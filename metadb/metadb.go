package metadb

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/indextree"
	"github.com/smartbch/moeingads/types"
)

const (
	ByteCurrHeight         = byte(0x10)
	ByteTwigMtFileSize     = byte(0x11)
	ByteEntryFileSize      = byte(0x12)
	ByteTwigHeight         = byte(0x13)
	ByteLastPrunedTwig     = byte(0x14)
	ByteEdgeNodes          = byte(0x15)
	ByteMaxSerialNum       = byte(0x16)
	ByteOldestActiveTwigID = byte(0x17)
	ByteIsRunning          = byte(0x18) //Not Used
	ByteRootHash           = byte(0x19)
)

type MetaDB struct {
	kvdb               *indextree.RocksDB
	currHeight         int64
	lastPrunedTwig     [types.ShardCount]int64
	maxSerialNum       [types.ShardCount]int64
	oldestActiveTwigID [types.ShardCount]int64
	rootHash           [types.ShardCount][32]byte
	mtx                sync.Mutex
}

var _ types.MetaDB = (*MetaDB)(nil)

func NewMetaDB(kvdb *indextree.RocksDB) *MetaDB {
	return &MetaDB{kvdb: kvdb}
}

func (db *MetaDB) Close() {
	db.kvdb = nil
}

func (db *MetaDB) ReloadFromKVDB() {
	db.currHeight = 0
	for i := 0; i < types.ShardCount; i++ {
		db.lastPrunedTwig[i] = 0
		db.maxSerialNum[i] = 0
		db.oldestActiveTwigID[i] = 0
	}

	bz := db.kvdb.Get([]byte{ByteCurrHeight})
	if bz != nil {
		db.currHeight = int64(binary.LittleEndian.Uint64(bz))
	}

	for i := 0; i < types.ShardCount; i++ {
		bz = db.kvdb.Get([]byte{ByteLastPrunedTwig, byte(i)})
		if bz != nil {
			db.lastPrunedTwig[i] = int64(binary.LittleEndian.Uint64(bz))
		}

		bz = db.kvdb.Get([]byte{ByteMaxSerialNum, byte(i)})
		if bz != nil {
			db.maxSerialNum[i] = int64(binary.LittleEndian.Uint64(bz))
		}

		bz = db.kvdb.Get([]byte{ByteOldestActiveTwigID, byte(i)})
		if bz != nil {
			db.oldestActiveTwigID[i] = int64(binary.LittleEndian.Uint64(bz))
		}

		bz = db.kvdb.Get([]byte{ByteRootHash, byte(i)})
		if bz != nil {
			copy(db.rootHash[i][:], bz)
		}
	}
}

func (db *MetaDB) Commit() {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(db.currHeight))
	db.kvdb.CurrBatch().Set([]byte{ByteCurrHeight}, buf[:])

	for i := 0; i < types.ShardCount; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(db.lastPrunedTwig[i]))
		db.kvdb.CurrBatch().Set([]byte{ByteLastPrunedTwig, byte(i)}, buf[:])

		binary.LittleEndian.PutUint64(buf[:], uint64(db.maxSerialNum[i]))
		db.kvdb.CurrBatch().Set([]byte{ByteMaxSerialNum, byte(i)}, buf[:])

		binary.LittleEndian.PutUint64(buf[:], uint64(db.oldestActiveTwigID[i]))
		db.kvdb.CurrBatch().Set([]byte{ByteOldestActiveTwigID, byte(i)}, buf[:])

		db.kvdb.CurrBatch().Set([]byte{ByteRootHash, byte(i)}, db.rootHash[i][:])
	}
}

func (db *MetaDB) SetCurrHeight(h int64) {
	db.currHeight = h
}

func (db *MetaDB) GetCurrHeight() int64 {
	return db.currHeight
}

func (db *MetaDB) SetTwigMtFileSize(shardID int, size int64) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.kvdb.CurrBatch().Set([]byte{ByteTwigMtFileSize, byte(shardID)}, buf[:])
}

func (db *MetaDB) GetTwigMtFileSize(shardID int) int64 {
	bz := db.kvdb.Get([]byte{ByteTwigMtFileSize, byte(shardID)})
	if bz != nil {
		return int64(binary.LittleEndian.Uint64(bz))
	}
	return 0
}

func (db *MetaDB) SetEntryFileSize(shardID int, size int64) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.kvdb.CurrBatch().Set([]byte{ByteEntryFileSize, byte(shardID)}, buf[:])
}

func (db *MetaDB) GetEntryFileSize(shardID int) int64 {
	bz := db.kvdb.Get([]byte{ByteEntryFileSize, byte(shardID)})
	if bz != nil {
		return int64(binary.LittleEndian.Uint64(bz))
	}
	return 0
}

func (db *MetaDB) setTwigHeight(shardID int, twigID int64, height int64) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteTwigHeight, byte(shardID)}, buf[:]...)
	binary.LittleEndian.PutUint64(buf[:], uint64(height))
	db.kvdb.CurrBatch().Set(key, buf[:])
}

func (db *MetaDB) GetTwigHeight(shardID int, twigID int64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	bz := db.kvdb.Get(append([]byte{ByteTwigHeight, byte(shardID)}, buf[:]...))
	if len(bz) == 0 {
		return -1
	}
	return int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDB) DeleteTwigHeight(shardID int, twigID int64) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	db.kvdb.CurrBatch().Delete(append([]byte{ByteTwigHeight, byte(shardID)}, buf[:]...))
}

func (db *MetaDB) SetLastPrunedTwig(shardID int, twigID int64) {
	db.lastPrunedTwig[shardID] = twigID
}

func (db *MetaDB) GetLastPrunedTwig(shardID int) int64 {
	return db.lastPrunedTwig[shardID]
}

func (db *MetaDB) GetEdgeNodes(shardID int) []byte {
	return db.kvdb.Get([]byte{ByteEdgeNodes, byte(shardID)})
}

func (db *MetaDB) SetEdgeNodes(shardID int, bz []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.kvdb.CurrBatch().Set([]byte{ByteEdgeNodes, byte(shardID)}, bz)
}

func (db *MetaDB) GetMaxSerialNum(shardID int) int64 {
	return db.maxSerialNum[shardID]
}

func (db *MetaDB) GetYoungestTwigID(shardID int) int64 {
	return db.maxSerialNum[shardID] >> datatree.TwigShift
}

func (db *MetaDB) IncrMaxSerialNum(shardID int) {
	db.maxSerialNum[shardID]++
	if db.maxSerialNum[shardID]%datatree.LeafCountInTwig == 0 {
		twigID := db.maxSerialNum[shardID] / datatree.LeafCountInTwig
		db.setTwigHeight(shardID, twigID, db.currHeight)
	}
}

func (db *MetaDB) GetRootHash(shardID int) [32]byte {
	return db.rootHash[shardID]
}

func (db *MetaDB) SetRootHash(shardID int, h [32]byte) {
	db.rootHash[shardID] = h
}

func (db *MetaDB) GetOldestActiveTwigID(shardID int) int64 {
	return db.oldestActiveTwigID[shardID]
}

func (db *MetaDB) IncrOldestActiveTwigID(shardID int) {
	db.oldestActiveTwigID[shardID]++
}

func (db *MetaDB) Init() {
	db.currHeight = 0
	for i := 0; i < types.ShardCount; i++ {
		db.lastPrunedTwig[i] = -1
		db.maxSerialNum[i] = 0
		db.oldestActiveTwigID[i] = 0
		db.SetTwigMtFileSize(i, 0)
		db.SetEntryFileSize(i, 0)
	}
	db.Commit()
}

func (db *MetaDB) PrintInfo() {
	fmt.Printf("CurrHeight         %v\n", db.GetCurrHeight())
	for i := 0; i < types.ShardCount; i++ {
		fmt.Printf("TwigMtFileSize     %d %v\n", i, db.GetTwigMtFileSize(i))
		fmt.Printf("EntryFileSize      %d %v\n", i, db.GetEntryFileSize(i))
		fmt.Printf("LastPrunedTwig     %d %v\n", i, db.GetLastPrunedTwig(i))
		fmt.Printf("EdgeNodes          %d %v\n", i, db.GetEdgeNodes(i))
		fmt.Printf("MaxSerialNum       %d %v\n", i, db.GetMaxSerialNum(i))
		fmt.Printf("OldestActiveTwigID %d %v\n", i, db.GetOldestActiveTwigID(i))
		fmt.Printf("RootHash           %d %#v\n", i, db.GetRootHash(i))
	}
}
