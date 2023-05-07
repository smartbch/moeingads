package indextree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// checks if height h exists in caches
// returns either true if it exists, vice versa
func isHeightExist(rc *RecentCache, h int64) (foundIt bool) {
	_, found := rc.caches[h]
	return found
}

func TestRecentCache(t *testing.T) {
	// create recent cache
	rc := NewRecentCache()
	caches := rc.caches

	/*
	CACHE METHODS
	*/
	// NewCache
	// fill in with caches
	caches[0] = NewCache()
	caches[1] = NewCache()
	caches[2] = NewCache()

	// Set
	// set values in caches
	caches[0].Set(0, 0)
	caches[0].Set(1, 1)
	caches[0].Set(2, 2)
	caches[1].Set(3, 3)
	caches[1].Set(4, 4)
	caches[1].Set(5, 5)
	caches[2].Set(6, 6)
	caches[2].Set(7, 7)
	caches[2].Set(8, 8)

	// Get
	// get values from caches
	value, _ := caches[0].Get(0)
	assert.Equal(t, value, int64(0))
	value, _ = caches[0].Get(1)
	assert.Equal(t, value, int64(1))
	value, _ = caches[0].Get(2)
	assert.Equal(t, value, int64(2))
	value, _ = caches[1].Get(3)
	assert.Equal(t, value, int64(3))
	value, _ = caches[1].Get(4)
	assert.Equal(t, value, int64(4))
	value, _ = caches[1].Get(5)
	assert.Equal(t, value, int64(5))
	value, _ = caches[2].Get(6)
	assert.Equal(t, value, int64(6))
	value, _ = caches[2].Get(7)
	assert.Equal(t, value, int64(7))
	value, _ = caches[2].Get(8)
	assert.Equal(t, value, int64(8))

	/*
	RECENTCACHE METHODS
	*/
	// Prune
	// delete height 1 from recentCache's caches map
	rc.Prune(1)
	// check that height 1 is gone
	foundIt := isHeightExist(rc, 1)
	assert.Equal(t, foundIt, false)
	// 0 and 2 still exist
	foundIt = isHeightExist(rc, 0)
	assert.Equal(t, foundIt, true)
	foundIt = isHeightExist(rc, 2)
	assert.Equal(t, foundIt, true)

	// SetAtHeight
	rc.SetAtHeight(3, 0, 0)
	rc.SetAtHeight(3, 1, 1)
	rc.SetAtHeight(4, 2, 2)
	rc.SetAtHeight(4, 3, 3)
	rc.SetAtHeight(5, 4, 4)
	rc.SetAtHeight(5, 5, 5)

	// FindFrom
	value, _ = rc.FindFrom(2, 6, 6)

	assert.Equal(t, value, int64(6))
	value, _ = rc.FindFrom(5, 6, 4)
	assert.Equal(t, value, int64(4))

	// cases in which cache cannot be retrieved from inputte height (err)
	_, foundIt = rc.FindFrom(6, 10, 0)
	assert.Equal(t, foundIt, false)
	// cases in which inputted key is not in inputted height
	// which leads to going up in height (loop)
	value, _ = rc.FindFrom(0, 5, 5)
	assert.Equal(t, value, int64(5))
}
