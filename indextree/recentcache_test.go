/*
Oct 10th: got ok but need to think about edge cases and test them
*/

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
	rc := RecentCache{}
	rc.caches = make(map[int64]*Cache)
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
	// ***WORKONNEXT: add more Pruning
	rc.Prune(1)
	// t.Log("PruneDONE==============================================================================")
	// check that height 1 is gone
	foundIt := isHeightExist(&rc, 1)
	assert.Equal(t, foundIt, false)
	// 0 and 2 still exist
	foundIt = isHeightExist(&rc, 0)
	assert.Equal(t, foundIt, true)
	foundIt = isHeightExist(&rc, 2)
	assert.Equal(t, foundIt, true)
	// t.Log("CheckHeightExistDONE==============================================================================")

	// AllocateIfNotExist
	// create new height in caches, 3, 4 & 5
	// ***WORKONNEXT: check that it does not make a duplicate for existing height
	rc.AllocateIfNotExist(3)
	rc.AllocateIfNotExist(4)
	rc.AllocateIfNotExist(5)
	// t.Log("AllocateIfNotExistDONE==============================================================================")

	// SetAtHeight
	rc.SetAtHeight(3, 0, 0)
	rc.SetAtHeight(3, 1, 1)
	rc.SetAtHeight(4, 2, 2)
	rc.SetAtHeight(4, 3, 3)
	rc.SetAtHeight(5, 4, 4)
	rc.SetAtHeight(5, 5, 5)
	// t.Log("SetAtHeightDONE==============================================================================")

	// recreate height 1 to test DidNotTouchInRange
	// can not have any "hole" while looping
	rc.AllocateIfNotExist(1)

	// check that all heights from 0 to 5 are occupied
	// t.Log(caches)

	// DidNotTouchInRange
	// need to set only up to 6 because max height is 5 right now (up to h < end)
	// there is a cache with key 3
	assert.Equal(t, rc.DidNotTouchInRange(0, 6, 3), false)
	// there is no cache with key 10
	assert.Equal(t, rc.DidNotTouchInRange(0, 6, 100), true)
	// t.Log("DidNotTouchInRangeDONE==============================================================================")

	// FindFrom
	// ***WORKONNEXT: edge cases like heights with same keys but different values
	value, _ = rc.FindFrom(2, 6, 6)

	assert.Equal(t, value, int64(6))
	value, _ = rc.FindFrom(5, 6, 4)
	assert.Equal(t, value, int64(4))
	// t.Log("FindFromDONE==============================================================================")
}
