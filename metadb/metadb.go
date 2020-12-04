package metadb

import (
	"fmt"
	"encoding/binary"

	"github.com/moeing-chain/MoeingADS/indextree"
	"github.com/moeing-chain/MoeingADS/datatree"
	"github.com/moeing-chain/MoeingADS/types"
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
	ByteIsRunning          = byte(0x18)
	//ByteActiveEntryCount   = byte(0x19)
)

type MetaDBWithTMDB struct {
	kvdb  *indextree.RocksDB

	currHeight         int64
	lastPrunedTwig     int64
	maxSerialNum       int64
	oldestActiveTwigID int64
	//activeEntryCount   int64
}

var _ types.MetaDB = (*MetaDBWithTMDB)(nil)

func NewMetaDB(kvdb *indextree.RocksDB) *MetaDBWithTMDB {
	return &MetaDBWithTMDB{kvdb: kvdb}
}

func (db *MetaDBWithTMDB) Close() {
	db.kvdb = nil
}

func (db *MetaDBWithTMDB) ReloadFromKVDB() {
	db.currHeight         = 0
	db.lastPrunedTwig     = 0
	db.maxSerialNum       = 0
	db.oldestActiveTwigID = 0
	//db.activeEntryCount   = 0

	bz := db.kvdb.Get([]byte{ByteCurrHeight})
	if bz != nil {
		db.currHeight = int64(binary.LittleEndian.Uint64(bz))
	}

	bz = db.kvdb.Get([]byte{ByteLastPrunedTwig})
	if bz != nil {
		db.lastPrunedTwig = int64(binary.LittleEndian.Uint64(bz))
	}

	bz = db.kvdb.Get([]byte{ByteMaxSerialNum})
	if bz != nil {
		db.maxSerialNum = int64(binary.LittleEndian.Uint64(bz))
	}

	bz = db.kvdb.Get([]byte{ByteOldestActiveTwigID})
	if bz != nil {
		db.oldestActiveTwigID = int64(binary.LittleEndian.Uint64(bz))
	}

	//bz = db.kvdb.Get([]byte{ByteActiveEntryCount})
	//if bz != nil {
	//	db.activeEntryCount = int64(binary.LittleEndian.Uint64(bz))
	//}
}

func (db *MetaDBWithTMDB) Commit() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(db.currHeight))
	db.kvdb.CurrBatch().Set([]byte{ByteCurrHeight}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.lastPrunedTwig))
	db.kvdb.CurrBatch().Set([]byte{ByteLastPrunedTwig}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.maxSerialNum))
	db.kvdb.CurrBatch().Set([]byte{ByteMaxSerialNum}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.oldestActiveTwigID))
	db.kvdb.CurrBatch().Set([]byte{ByteOldestActiveTwigID}, buf[:])

	//binary.LittleEndian.PutUint64(buf[:], uint64(db.activeEntryCount))
	//db.kvdb.CurrBatch().Set([]byte{ByteActiveEntryCount}, buf[:])
}

func (db *MetaDBWithTMDB) SetCurrHeight(h int64) {
	db.currHeight = h
}

func (db *MetaDBWithTMDB) GetCurrHeight() int64 {
	return db.currHeight
}

func (db *MetaDBWithTMDB) SetTwigMtFileSize(size int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.kvdb.CurrBatch().Set([]byte{ByteTwigMtFileSize}, buf[:])
}

func (db *MetaDBWithTMDB) GetTwigMtFileSize() int64 {
	bz := db.kvdb.Get([]byte{ByteTwigMtFileSize})
	if bz != nil {
		return int64(binary.LittleEndian.Uint64(bz))
	}
	return 0
}

func (db *MetaDBWithTMDB) SetEntryFileSize(size int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.kvdb.CurrBatch().Set([]byte{ByteEntryFileSize}, buf[:])
}

func (db *MetaDBWithTMDB) GetEntryFileSize() int64 {
	bz := db.kvdb.Get([]byte{ByteEntryFileSize})
	if bz != nil {
		return int64(binary.LittleEndian.Uint64(bz))
	}
	return 0
}

func (db *MetaDBWithTMDB) setTwigHeight(twigID int64, height int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteTwigHeight}, buf[:]...)
	binary.LittleEndian.PutUint64(buf[:], uint64(height))
	db.kvdb.CurrBatch().Set(key, buf[:])
}

func (db *MetaDBWithTMDB) GetTwigHeight(twigID int64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	bz := db.kvdb.Get(append([]byte{ByteTwigHeight}, buf[:]...))
	if len(bz) == 0 {
		return -1
	}
	return int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) DeleteTwigHeight(twigID int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	db.kvdb.Delete(append([]byte{ByteTwigHeight}, buf[:]...))
}

func (db *MetaDBWithTMDB) SetLastPrunedTwig(twigID int64) {
	db.lastPrunedTwig = twigID
}

func (db *MetaDBWithTMDB) GetLastPrunedTwig() int64 {
	return db.lastPrunedTwig
}

func (db *MetaDBWithTMDB) GetEdgeNodes() []byte {
	return db.kvdb.Get([]byte{ByteEdgeNodes})
}

func (db *MetaDBWithTMDB) SetEdgeNodes(bz []byte) {
	db.kvdb.Set([]byte{ByteEdgeNodes}, bz)
}

func (db *MetaDBWithTMDB) GetMaxSerialNum() int64 {
	return db.maxSerialNum
}

func (db *MetaDBWithTMDB) IncrMaxSerialNum() {
	db.maxSerialNum++
	if db.maxSerialNum%datatree.LeafCountInTwig == 0 {
		twigID := db.maxSerialNum / datatree.LeafCountInTwig
		db.setTwigHeight(twigID, db.currHeight)
	}
}

//func (db *MetaDBWithTMDB) GetActiveEntryCount() int64 {
//	return db.activeEntryCount
//}
//
//func (db *MetaDBWithTMDB) IncrActiveEntryCount() {
//	db.activeEntryCount++
//}
//
//func (db *MetaDBWithTMDB) DecrActiveEntryCount() {
//	db.activeEntryCount--
//}

func (db *MetaDBWithTMDB) GetOldestActiveTwigID() int64 {
	return db.oldestActiveTwigID
}

func (db *MetaDBWithTMDB) IncrOldestActiveTwigID() {
	db.oldestActiveTwigID++
}

func (db *MetaDBWithTMDB) GetIsRunning() bool {
	bz := db.kvdb.Get([]byte{ByteIsRunning})
	return len(bz) == 0 || bz[0] != 0
}

func (db *MetaDBWithTMDB) SetIsRunning(isRunning bool) {
	if isRunning {
		db.kvdb.SetSync([]byte{ByteIsRunning}, []byte{1})
	} else {
		db.kvdb.SetSync([]byte{ByteIsRunning}, []byte{0})
	}
}

func (db *MetaDBWithTMDB) Init() {
	db.SetIsRunning(false)
	db.currHeight = 0
	db.lastPrunedTwig = -1
	db.maxSerialNum = 0
	db.oldestActiveTwigID = 0
	//db.activeEntryCount = 0
	db.SetTwigMtFileSize(0)
	db.SetEntryFileSize(0)
	db.Commit()
}

func (db *MetaDBWithTMDB) PrintInfo() {
	fmt.Printf("CurrHeight         %v\n", db.GetCurrHeight())
	fmt.Printf("TwigMtFileSize     %v\n", db.GetTwigMtFileSize())
	fmt.Printf("EntryFileSize      %v\n", db.GetEntryFileSize())
	fmt.Printf("LastPrunedTwig     %v\n", db.GetLastPrunedTwig())
	fmt.Printf("EdgeNodes          %v\n", db.GetEdgeNodes())
	fmt.Printf("MaxSerialNum       %v\n", db.GetMaxSerialNum())
	fmt.Printf("OldestActiveTwigID %v\n", db.GetOldestActiveTwigID())
	fmt.Printf("IsRunning          %v\n", db.GetIsRunning())
}
