package store

import (
	"github.com/smartbch/moeingads/store/types"
)

// CacheStore is used in two places: 1. as the temporal cache in TrunkStore,
// and 2. as the underlying storage in MockRootStore
// The 'Delete' function is used in place 1, which only marks a KV pair should
// be later deleted in the underlying RootStore.
// THe 'RealDelete' function is used in place 2.
type CacheStore struct {
	m map[string]string
}

func NewCacheStore() *CacheStore {
	return &CacheStore{
		m: make(map[string]string),
	}
}

func (cs *CacheStore) Size() int {
	return len(cs.m)
}

func (cs *CacheStore) Close() {
	cs.m = nil
}

func (cs *CacheStore) ScanAllEntries(fn func(k, v []byte, isDeleted bool)) {
	for key, value := range cs.m {
		fn([]byte(key), []byte(value), len(value) == 0)
	}
}

func (cs *CacheStore) Get(key []byte) (res []byte, status types.CacheStatus) {
	v, ok := cs.m[string(key)]
	if !ok {
		status = types.Missed
	} else if len(v) == 0 {
		status = types.JustDeleted
	} else {
		res = []byte(v)
		status = types.Hit
	}
	return
}

func (cs *CacheStore) Set(key, value []byte) {
	cs.m[string(key)] = string(value)
}

func (cs *CacheStore) RealDelete(key []byte) {
	delete(cs.m, string(key))
}

func (cs *CacheStore) Delete(key []byte) {
	cs.m[string(key)] = ""
}
