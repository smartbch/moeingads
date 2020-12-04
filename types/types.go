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
	Key []byte
	Pos int64
}

type OperationOnEntry int32

const (
	OpNone OperationOnEntry = iota
	OpDelete
	OpInsertOrChange
)

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
	Value() uint64
	Close()
}

type IndexTree interface {
	Init(repFn func([]byte)) error
	ActiveCount() int
	BeginWrite(height int64)
	EndWrite()
	Iterator(start, end []byte) IteratorUI64
	ReverseIterator(start, end []byte) IteratorUI64
	Get(k []byte) (uint64, bool)
	GetAtHeight(k []byte, height uint64) (uint64, bool)
	Set(k []byte, v uint64)
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
	EndBlock() []byte
	Flush()
	Close()
}

type MetaDB interface {
	Commit()
	ReloadFromKVDB()
	PrintInfo()

	SetCurrHeight(h int64)
	GetCurrHeight() int64

	SetTwigMtFileSize(size int64)
	GetTwigMtFileSize() int64

	SetEntryFileSize(size int64)
	GetEntryFileSize() int64

	GetTwigHeight(twigID int64) int64
	DeleteTwigHeight(twigID int64)

	SetLastPrunedTwig(twigID int64)
	GetLastPrunedTwig() int64

	GetEdgeNodes() []byte
	SetEdgeNodes(bz []byte)

	// MaxSerialNum is the maximum serial num among all the entries
	GetMaxSerialNum() int64
	IncrMaxSerialNum() // It should call setTwigHeight(twigID int64, height int64)

	//// the count of all the active entries, increased in AppendEntry, decreased in DeactiviateEntry
	//GetActiveEntryCount() int64
	//IncrActiveEntryCount()
	//DecrActiveEntryCount()

	// the ID of the oldest active twig, increased by ReapOldestActiveTwig
	GetOldestActiveTwigID() int64
	IncrOldestActiveTwigID()

	GetIsRunning() bool
	SetIsRunning(isRunning bool)

	Init()
	Close()
}
