package indextree

import (
	"fmt"
	"sort"

	"github.com/smartbch/moeingads/types"
)

type Cache struct {
	maps map[uint64]int64
}

func NewCache() *Cache {
	return &Cache{
		maps: make(map[uint64]int64),
	}
}

func (cache *Cache) Dump() {
	keys := make([]uint64, 0, len(cache.maps))
	for key := range cache.maps {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
	for _, k := range keys {
		fmt.Printf(" k %x v %d\n", k, cache.maps[k])
	}
}

func (cache *Cache) Set(k uint64, v int64) {
	cache.maps[k] = v
}

func (cache *Cache) Get(k uint64) (v int64, ok bool) {
	v, ok = cache.maps[k]
	return
}

type RecentCache struct {
	caches [types.IndexChanCount]map[int64]*Cache
}

func NewRecentCache() *RecentCache {
	res := &RecentCache{}
	for i := range res.caches {
		res.caches[i] = make(map[int64]*Cache)
	}
	return res
}

func (rc *RecentCache) Dump(start, end int64) {
	for h := start; h < end; h++ {
		for j := 0; j < types.IndexChanCount; j++ {
			cache, ok := rc.caches[j][h]
			if !ok {
				continue
			}
			fmt.Printf("height %d chanId %d\n", h, j)
			cache.Dump()
		}
	}
}

func (rc *RecentCache) Prune(h int64) {
	for i := range rc.caches {
		delete(rc.caches[i], h)
	}
}

func (rc *RecentCache) allocateIfNotExist(idxChanId int, h int64) {
	if _, ok := rc.caches[idxChanId][h]; !ok {
		rc.caches[idxChanId][h] = NewCache()
	}
}

func (rc *RecentCache) SetAtHeight(h int64, k uint64, v int64) {
	idxChanId := types.GetIndexChanID(byte(k>>56))
	rc.allocateIfNotExist(idxChanId, h)
	rc.caches[idxChanId][h].Set(k, v)
}

func (rc *RecentCache) GetAtHeight(h int64, k uint64) (v int64, ok bool) {
	idxChanId := types.GetIndexChanID(byte(k>>56))
	rc.allocateIfNotExist(idxChanId, h)
	v, ok = rc.caches[idxChanId][h].Get(k)
	return
}

func (rc *RecentCache) FindFrom(height, endHeight int64, k uint64) (v int64, foundIt bool) {
	idxChanId := types.GetIndexChanID(byte(k>>56))
	for height <= endHeight {
		cache, ok := rc.caches[idxChanId][height]
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
