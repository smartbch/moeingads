package store

import (
	"fmt"
	"sync"

	"github.com/smartbch/moeingads/store/types"
)

// MockRootStore mimics RootStore's behavior, with an in-memory CacheStore
type MockRootStore struct {
	mtx                 sync.RWMutex
	cacheStore          *CacheStore
	preparedForUpdate   *sync.Map
	preparedForDeletion *sync.Map
	isWritting          bool
}

var _ types.RootStoreI = &MockRootStore{}

func NewMockRootStore() *MockRootStore {
	return &MockRootStore{
		cacheStore:          NewCacheStore(1000),
		preparedForUpdate:   &sync.Map{},
		preparedForDeletion: &sync.Map{},
	}
}

func (rs *MockRootStore) RLock() {
	rs.mtx.RLock()
}

func (rs *MockRootStore) Lock() {
	rs.mtx.Lock()
}

func (rs *MockRootStore) RUnlock() {
	rs.mtx.RUnlock()
}

func (rs *MockRootStore) Unlock() {
	rs.mtx.Unlock()
}

func (rs *MockRootStore) GetTrunkStore(cacheSize int) interface{} {
	return &TrunkStore{
		cache:     NewCacheStore(cacheSize),
		root:      rs,
		isWriting: 0,
	}
}

func (rs *MockRootStore) SetHeight(h int64) {
}

func (rs *MockRootStore) GetAtHeight(key []byte, height uint64) []byte {
	panic("Implement me")
}

func (rs *MockRootStore) Get(key []byte) []byte {
	if rs.isWritting {
		panic("isWritting")
	}
	v, status := rs.cacheStore.Get(key)
	if status == types.Missed {
		return nil
	}
	return v
}

func (rs *MockRootStore) Has(key []byte) bool {
	if rs.isWritting {
		panic("isWritting")
	}
	_, status := rs.cacheStore.Get(key)
	return status == types.Hit
}

func (rs *MockRootStore) PrepareForUpdate(key []byte) {
	if rs.isWritting {
		panic("isWritting")
	}
	rs.preparedForUpdate.Store(string(key), struct{}{})
}

func (rs *MockRootStore) PrepareForDeletion(key []byte) {
	if rs.isWritting {
		panic("isWritting")
	}
	rs.preparedForDeletion.Store(string(key), struct{}{})
}

func (rs *MockRootStore) BeginWrite() {
	rs.isWritting = true
}

func (rs *MockRootStore) Set(key, value []byte) {
	if !rs.isWritting {
		panic("notWritting")
	}
	if _, ok := rs.preparedForUpdate.Load(string(key)); !ok {
		panic(fmt.Sprintf("not prepared %#v", key))
	}
	rs.cacheStore.Set(key, value)
}

func (rs *MockRootStore) Delete(key []byte) {
	if !rs.isWritting {
		panic("notWritting")
	}
	if _, ok := rs.preparedForDeletion.Load(string(key)); !ok {
		panic("not prepared")
	}
	rs.cacheStore.RealDelete(key)
}

func (rs *MockRootStore) EndWrite() {
	rs.isWritting = false
	rs.preparedForUpdate = &sync.Map{}
	rs.preparedForDeletion = &sync.Map{}
}

func (rs *MockRootStore) Update(updater func(db types.SetDeleter)) {
	rs.BeginWrite()
	updater(rs)
	rs.EndWrite()
}

func (rs *MockRootStore) CheckConsistency() {
}

func (rs *MockRootStore) Close() {
}

func (rs *MockRootStore) ActiveCount() int {
	return rs.cacheStore.Size()
}
