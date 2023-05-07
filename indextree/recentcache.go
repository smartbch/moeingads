package indextree

import (
	"github.com/smartbch/moeingads/types"
)

type Cache struct {
	maps [types.IndexChanCount]map[uint64]int64
}

func NewCache() *Cache {
	res := &Cache{}
	for i := range res.maps {
		res.maps[i] = make(map[uint64]int64)
	}
	return res
}

func (cache *Cache) Set(k uint64, v int64) {
	m := cache.maps[types.GetIndexChanID(byte(k>>56))]
	m[k] = v
}

func (cache *Cache) Get(k uint64) (v int64, ok bool) {
	m := cache.maps[types.GetIndexChanID(byte(k>>56))]
	v, ok = m[k]
	return
}

type RecentCache struct {
	caches map[int64]*Cache
}

func NewRecentCache() *RecentCache {
	return &RecentCache{caches: make(map[int64]*Cache)}
}

func (rc *RecentCache) Prune(h int64) {
	delete(rc.caches, h)
}

func (rc *RecentCache) allocateIfNotExist(h int64) {
	if _, ok := rc.caches[h]; !ok {
		rc.caches[h] = NewCache()
	}
}

func (rc *RecentCache) SetAtHeight(h int64, k uint64, v int64) {
	rc.allocateIfNotExist(h)
	rc.caches[h].Set(k, v)
}

func (rc *RecentCache) FindFrom(height, endHeight int64, k uint64) (v int64, foundIt bool) {
	for height <= endHeight {
		cache, ok := rc.caches[height]
		if !ok {
			height++
			continue
		}
		v, ok := cache.Get(k)
		if ok {
			return v, true
		}
		height++
	}
	return
}
