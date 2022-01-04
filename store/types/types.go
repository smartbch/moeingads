package types

type Serializable interface {
	ToBytes() []byte
	FromBytes([]byte)
	DeepCopy() interface{}
}

type CacheStatus int

const (
	//nolint
	Missed      CacheStatus = 0
	Hit         CacheStatus = 1
	JustDeleted CacheStatus = -1
)

type SetDeleter interface {
	Set(key, value []byte)
	Delete(key []byte)
}

type BaseStoreI interface {
	RLock()
	RUnlock()
	Get(key []byte) []byte
	GetAtHeight(key []byte, height uint64) []byte
	PrepareForUpdate(key []byte)
	PrepareForDeletion(key []byte)
	Update(func(db SetDeleter))
	ActiveCount() int
}

type RootStoreI interface {
	BaseStoreI
	SetDeleter
	GetTrunkStore(cacheSize int) interface{}
	SetHeight(h int64)
	BeginWrite()
	EndWrite()
	Close()
	Lock()
	Unlock()
}
