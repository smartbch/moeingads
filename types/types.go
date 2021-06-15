package types

type Entry struct {
	Key        []byte
	Value      []byte
	NextKey    []byte
	Height     int64
	LastHeight int64
	SerialNum  int64
}

type EntryX struct {
	Entry           *Entry
	Pos             int64
	DeactivedSNList []int64
}

type KeyAndPos struct {
	Key       []byte
	Pos       int64
	SerialNum int64
}

type OperationOnEntry int32

const (
	OpNone OperationOnEntry = iota
	OpDelete
	OpInsertOrChange

	ShardCount = 8
)

func LimitRange(b byte) byte {
	return (b % 128) + 64 // limit the range to avoid start&end Guard
}

func GetShardID(b byte) int {
	if b < 64 {
		return 0
	} else if b >= 128 + 64 {
		return 7
	} else { // make shards in the limited range
		return (int(b) - 64) / 16
	}
}

type HotEntry struct {
	EntryPtr        *Entry
	Operation       OperationOnEntry
	IsModified      bool
	IsTouchedByNext bool
}

type IteratorUI64 interface {
	Domain() (start []byte, end []byte)
	Valid() bool
	Next()
	Key() []byte
	Value() int64
	Close()
}

type IndexTree interface {
	Init(repFn func([]byte)) error
	ActiveCount() int
	BeginWrite(height int64)
	EndWrite()
	Iterator(start, end []byte) IteratorUI64
	ReverseIterator(start, end []byte) IteratorUI64
	Get(k []byte) (int64, bool)
	GetAtHeight(k []byte, height uint64) (int64, bool)
	Set(k []byte, v int64)
	Delete(k []byte)
	Close()
}

type EntryHandler func(pos int64, entry *Entry, deactivedSNList []int64)

type DataTree interface {
	DeactiviateEntry(sn int64) int
	AppendEntry(entry *Entry) int64
	AppendEntryRawBytes(entryBz []byte, sn int64) int64
	ReadEntry(pos int64) *Entry
	GetActiveBit(sn int64) bool
	EvictTwig(twigID int64)
	GetActiveEntriesInTwig(twigID int64) chan []byte
	ScanEntries(oldestActiveTwigID int64, outChan chan EntryX)
	ScanEntriesLite(oldestActiveTwigID int64, outChan chan KeyAndPos)
	TwigCanBePruned(twigID int64) bool
	PruneTwigs(startID, endID int64) []byte
	GetFileSizes() (int64, int64)
	EndBlock() [32]byte
	WaitForFlushing()
	DeactivedSNListSize() int
	PrintTree()
	Flush()
	Close()
}

type MetaDB interface {
	Commit()
	ReloadFromKVDB()
	PrintInfo()

	SetCurrHeight(h int64)
	GetCurrHeight() int64

	SetTwigMtFileSize(shardID int, size int64)
	GetTwigMtFileSize(shardID int) int64

	SetEntryFileSize(shardID int, size int64)
	GetEntryFileSize(shardID int) int64

	GetTwigHeight(shardID int, twigID int64) int64
	DeleteTwigHeight(shardID int, twigID int64)

	SetLastPrunedTwig(shardID int, twigID int64)
	GetLastPrunedTwig(shardID int) int64

	SetEdgeNodes(shardID int, bz []byte)
	GetEdgeNodes(shardID int) []byte

	// MaxSerialNum is the maximum serial num among all the entries
	GetMaxSerialNum(shardID int) int64
	IncrMaxSerialNum(shardID int) // It should call setTwigHeight(twigID int64, height int64)

	SetRootHash(shardID int, h [32]byte)
	GetRootHash(shardID int) [32]byte

	// the ID of the oldest active twig, increased by ReapOldestActiveTwig
	GetOldestActiveTwigID(shardID int) int64
	IncrOldestActiveTwigID(shardID int)

	Init()
	Close()
}
