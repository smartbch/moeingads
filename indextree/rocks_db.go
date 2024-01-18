package indextree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/smartbch/moeingads/types"
	"github.com/tecbot/gorocksdb"
)

// We use rocksdb's customizable compact filter to prune old records
type HeightCompactionFilter struct {
	pruneHeight uint64
	pruneEnable bool
}

func (f *HeightCompactionFilter) Name() string {
	return "HeightPruneFilter"
}

// The last 8 bytes of keys are expiring height. If it is too small, we prune the record
func (f *HeightCompactionFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	if len(key) < 8 {
		return false, val
	}
	if len(key) >= 1 && key[0] != 0 {
		return false, val // not starting with zero
	}
	start := len(key) - 8
	h := binary.BigEndian.Uint64(key[start:])
	if f.pruneEnable && f.pruneHeight > h {
		return true, nil
	} else {
		return false, val
	}
}

type RocksDB struct {
	db     *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	woSync *gorocksdb.WriteOptions
	filter *HeightCompactionFilter
	batch  *rocksDBBatch
	mtx    sync.Mutex
}

func (db *RocksDB) CurrBatch() types.Batch {
	return db.batch
}

func (db *RocksDB) LockBatch() {
	db.mtx.Lock()
}

func (db *RocksDB) UnlockBatch() {
	db.mtx.Unlock()
}

func (db *RocksDB) CloseOldBatch() {
	if db.batch != nil {
		db.batch.WriteSync()
		db.batch.Close()
		db.batch = nil
	}
}

func (db *RocksDB) OpenNewBatch() {
	batch := gorocksdb.NewWriteBatch()
	db.batch = &rocksDBBatch{db, batch}
}

func NewRocksDB(name string, dir string) (IKVDB, error) {
	// default rocksdb option, good enough for most cases, including heavy workloads.
	// 64MB table cache, 32MB write buffer
	// compression: snappy as default, need to -lsnappy to enable.
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024 * 1024))
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))

	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	// SetMaxOpenFiles to 4096 seems to provide a reliable performance boost
	opts.SetMaxOpenFiles(4096)
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())
	// 1.5GB maximum memory use for writebuffer.
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	return NewRocksDBWithOptions(name, dir, opts)
}

func NewRocksDBWithOptions(name string, dir string, opts *gorocksdb.Options) (IKVDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	filter := HeightCompactionFilter{}
	opts.SetCompactionFilter(&filter) // use a customized compaction filter
	opts.SetCompression(gorocksdb.NoCompression)
	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, err
	}
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	woSync := gorocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)
	database := &RocksDB{
		db:     db,
		ro:     ro,
		wo:     wo,
		woSync: woSync,
		filter: &filter,
	}
	return database, nil
}

func (db *RocksDB) SetPruneHeight(h uint64) {
	if db.filter.pruneHeight < h {
		db.filter.pruneHeight = h
	}
	db.filter.pruneEnable = true
}

func (db *RocksDB) GetPruneHeight() (uint64, bool) {
	return db.filter.pruneHeight, db.filter.pruneEnable
}

// Implements DB.
func (db *RocksDB) Get(key []byte) []byte {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	res, err := db.db.Get(db.ro, key)
	if err != nil {
		panic(err)
	}
	return moveSliceToBytes(res)
}

// Implements DB.
func (db *RocksDB) Has(key []byte) bool {
	bytes := db.Get(key)
	return bytes != nil
}

// Implements DB.
func (db *RocksDB) Set(key []byte, value []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if value == nil {
		panic("Nil Value for RocksDB")
	}
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) SetSync(key []byte, value []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if value == nil {
		panic("Nil Value for RocksDB")
	}
	err := db.db.Put(db.woSync, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) Delete(key []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	err := db.db.Delete(db.wo, key)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) DeleteSync(key []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	err := db.db.Delete(db.woSync, key)
	if err != nil {
		panic(err)
	}
}

func (db *RocksDB) DB() *gorocksdb.DB {
	return db.db
}

// Implements DB.
func (db *RocksDB) Close() {
	db.ro.Destroy()
	db.wo.Destroy()
	db.woSync.Destroy()
	db.db.Close()
}

// Implements DB.
func (db *RocksDB) Print() {
	itr := db.Iterator(nil, nil)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
}

// Implements DB.
func (db *RocksDB) Stats() map[string]string {
	keys := []string{"rocksdb.stats"}
	stats := make(map[string]string, len(keys))
	for _, key := range keys {
		stats[key] = db.db.GetProperty(key)
	}
	return stats
}

//----------------------------------------
// Batch

// Implements DB.
func (db *RocksDB) NewBatch() types.Batch {
	batch := gorocksdb.NewWriteBatch()
	return &rocksDBBatch{db, batch}
}

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *rocksDBBatch) Set(key, value []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if value == nil {
		panic("Nil Value for RocksDB")
	}
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Delete(key []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Write() {
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
	if err != nil {
		panic(err)
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	mBatch.Close()
}

// Implements Batch.
func (mBatch *rocksDBBatch) WriteSync() {
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	err := mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch)
	if err != nil {
		panic(err)
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	mBatch.Close()
}

// Implements Batch.
func (mBatch *rocksDBBatch) Close() {
	if mBatch.batch != nil {
		mBatch.batch.Destroy()
		mBatch.batch = nil
	}
}

//----------------------------------------
// Iterator

func (db *RocksDB) Iterator(start, end []byte) types.Iterator {
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, false)
}

func (db *RocksDB) ReverseIterator(start, end []byte) types.Iterator {
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, true)
}

var _ types.Iterator = (*rocksDBIterator)(nil)

type rocksDBIterator struct {
	source     *gorocksdb.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ types.Iterator = (*rocksDBIterator)(nil)

func newRocksDBIterator(source *gorocksdb.Iterator, start, end []byte, isReverse bool) *rocksDBIterator {
	if isReverse {
		if end == nil {
			source.SeekToLast()
		} else {
			source.Seek(end)
			if source.Valid() {
				eoakey := moveSliceToBytes(source.Key()) // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &rocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr *rocksDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *rocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.source.Err(); err != nil {
		itr.isInvalid = true
		return false
	}

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var start = itr.start
	var end = itr.end
	var key = moveSliceToBytes(itr.source.Key())
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

func (itr *rocksDBIterator) Key() []byte {
	itr.assertIsValid()
	return moveSliceToBytes(itr.source.Key())
}

func (itr *rocksDBIterator) Value() []byte {
	itr.assertIsValid()
	return moveSliceToBytes(itr.source.Value())
}

func (itr rocksDBIterator) Next() {
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

func (itr *rocksDBIterator) Close() {
	itr.source.Close()
}

func (itr *rocksDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

// moveSliceToBytes will free the slice and copy out a go []byte
// This function can be applied on *Slice returned from Key() and Value()
// of an Iterator, because they are marked as freed.
func moveSliceToBytes(s *gorocksdb.Slice) []byte {
	defer s.Free()
	if !s.Exists() {
		return nil
	}
	v := make([]byte, len(s.Data()))
	copy(v, s.Data())
	return v
}
