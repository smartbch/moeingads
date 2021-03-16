package fuzz

import (
	"github.com/smartbch/MoeingADS/store"
	storetypes "github.com/smartbch/MoeingADS/store/types"
)

type UndoOp struct {
	oldStatus  storetypes.CacheStatus
	key, value []byte
}

type RefStore struct {
	cs *store.CacheStore
}

func NewRefStore() *RefStore {
	return &RefStore{
		cs: store.NewCacheStore(),
	}
}

func (rs *RefStore) Size() int {
	return rs.cs.Size()
}

func (rs *RefStore) Clone() *RefStore {
	newStore := NewRefStore()
	rs.cs.ScanAllEntries(func(key, value []byte, isDeleted bool) {
		if isDeleted {
			return
		}
		newStore.cs.Set(key, value)
	})
	return newStore
}

func (rs *RefStore) Close() {
	rs.cs.Close()
}

func (rs *RefStore) Get(key []byte) []byte {
	v, _ := rs.cs.Get(key)
	return v
}

func (rs *RefStore) Has(key []byte) bool {
	_, status := rs.cs.Get(key)
	return status != storetypes.Missed
}

func (rs *RefStore) RealSet(key, value []byte) {
	rs.cs.Set(key, value)
}

func (rs *RefStore) Set(key, value []byte) UndoOp {
	v, status := rs.cs.Get(key)
	rs.cs.Set(key, value)
	return UndoOp{
		oldStatus: status,
		key:       key,
		value:     v,
	}
}

func (rs *RefStore) RealDelete(key []byte) {
	rs.cs.RealDelete(key)
}

func (rs *RefStore) Delete(key []byte) UndoOp {
	v, status := rs.cs.Get(key)
	rs.cs.Delete(key)
	return UndoOp{
		oldStatus: status,
		key:       key,
		value:     v,
	}
}
