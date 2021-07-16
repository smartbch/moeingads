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
	m := cache.maps[types.GetIndexChanID(byte(k>>64))]
	m[k] = v
}

func (cache *Cache) Get(k uint64) (v int64, ok bool) {
	m := cache.maps[types.GetIndexChanID(byte(k>>64))]
	v, ok = m[k]
	return
}

type RecentCache struct {
	caches map[int64]*Cache
}

func (rc *RecentCache) Prune(h int64) {
	delete(rc.caches, h)
}

func (rc *RecentCache) AllocateIfNotExist(h int64) {
	if _, ok := rc.caches[h]; !ok {
		rc.caches[h] = NewCache()
	}
}

func (rc *RecentCache) DidNotTouchInRange(start, end int64, k uint64) bool {
	for h := start; h < end; h++ {
		if _, ok := rc.caches[h].Get(k); ok {
			return false
		}
	}
	return true
}

func (rc *RecentCache) FindFirstChangeBefore(height int64, k uint64) (v int64, foundIt bool) {
	for {
		cache, ok := rc.caches[height]
		if !ok {
			break
		}
		v, ok := cache.Get(k)
		if ok {
			return v, true
		}
	}
	return
}
