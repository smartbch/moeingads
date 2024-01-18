package indextree

import "github.com/smartbch/moeingads/types"

type IKVDB interface {
	Close()
	GetPruneHeight() (uint64, bool)
	NewBatch() types.Batch
	OpenNewBatch()
	CloseOldBatch()
	LockBatch()
	UnlockBatch()
	CurrBatch() types.Batch
	Get(key []byte) []byte
	Iterator(start, end []byte) types.Iterator
	ReverseIterator(start, end []byte) types.Iterator
	SetPruneHeight(h uint64)
}
