package rabbit

import (
	"fmt"

	"github.com/smartbch/moeingads/store/types"
)

// A SimpleMultiStore serves a transaction which uses 'RabbitJump' algorithm to access KVs.
// Its parent can be TrunkStore or RootStore, from which it reads data.
// It uses SimpleMultiStore to buffer the written data from a transaction, such that later
// SLOAD instructions can get the data written by previous SSTORE instructions.
// Before it is closed, it holds a read-lock of its parent. After it is closed, the cache's
// content can be written back to its parent. The write-back processes should be carefully
// managed by the client code to make sure there is no contention to the parent.
type SimpleMultiStore struct {
	cache    *SimpleCacheStore
	parent   types.BaseStoreI
	isClosed bool
}

//var WatchedKey = []uint8{0x47, 0x60, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0}
//var WatchedShortKey = []uint8{0xe3, 0x7e}

func NewSimpleMultiStore(parent types.BaseStoreI) SimpleMultiStore {
	parent.RLock()
	return SimpleMultiStore{
		cache:    NewSimpleCacheStore(),
		parent:   parent,
		isClosed: false,
	}
}

func (sms *SimpleMultiStore) GetCachedValue(key [KeySize]byte) *CachedValue {
	if sms.isClosed {
		panic("Cannot access after closed")
	}
	cv, status := sms.cache.GetValue(key)
	switch status {
	case types.JustDeleted:
		return nil
	case types.Missed:
		bz := sms.parent.Get(key[:])
		if bz == nil {
			return nil
		}
		cv := BytesToCachedValue(bz)
		sms.cache.SetValue(key, cv)
		return cv
	case types.Hit:
		return cv
	default:
		panic(fmt.Sprintf("Invalid Status %d", status))
	}
}

func (sms *SimpleMultiStore) MustGetCachedValue(key [KeySize]byte) *CachedValue {
	if sms.isClosed {
		panic("Cannot acccess after closed")
	}
	cv, status := sms.cache.GetValue(key)
	if status != types.Hit {
		panic("Failed to get cached value")
	}
	return cv
}

func (sms *SimpleMultiStore) SetCachedValue(key [KeySize]byte, cv *CachedValue) {
	if sms.isClosed {
		panic("Cannot acccess after closed")
	}
	cv.isDirty = true
	sms.cache.SetValue(key, cv)
	if cv.isDeleted {
		sms.parent.PrepareForDeletion(key[:])
	} else {
		sms.parent.PrepareForUpdate(key[:])
	}
}

func (sms *SimpleMultiStore) Close() {
	if sms.isClosed {
		return
	}
	sms.parent.RUnlock()
	sms.isClosed = true
}

func (sms *SimpleMultiStore) WriteBack() {
	if !sms.isClosed {
		panic("Cannot write back before closed")
	}
	sms.parent.Update(func(db types.SetDeleter) {
		sms.cache.ScanAllDirtyEntries(func(key, value []byte, isDeleted bool) {
			if isDeleted {
				db.Delete(key)
			} else {
				db.Set(key, value)
			}
		})
	})
}
