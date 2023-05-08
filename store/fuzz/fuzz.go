package fuzz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/coinexchain/randsrc"

	"github.com/smartbch/moeingads"
	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/store"
	"github.com/smartbch/moeingads/store/rabbit"
	storetypes "github.com/smartbch/moeingads/store/types"
)

// go test -tags debug -c -coverpkg github.com/smartbch/moeingads/... .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=2000 ./fuzz.test -test.coverprofile a.out

func allTests() {
	//cfg1 := &FuzzConfig {
	//	MaxReadCountInTx:       10,
	//	MaxCheckProofCountInTx: 2,
	//	MaxWriteCountInTx:      10,
	//	MaxDeleteCountInTx:     10,
	//	MaxTxCountInEpoch:      100,
	//	MaxEpochCountInBlock:   5,
	//	EffectiveBits:          0xFFF00000_00000FFF,
	//	MaxActiveCount:         -1,
	//	MaxValueLength:         256,
	//	TxSucceedRatio:         0.85,
	//	BlockSucceedRatio:      0.95,
	//	BlockPanicRatio:        0.02,
	//	RootType:               "Real",
	//	ConsistencyEveryNBlock: 200,
	//	PruneEveryNBlock:       100,
	//	KeepRecentNBlock:       100,
	//	ReloadEveryNBlock:      500,
	//}
	//runTest(cfg1)

	cfg2 := &FuzzConfig{
		MaxReadCountInTx:       20,
		MaxCheckProofCountInTx: 20,
		MaxWriteCountInTx:      10,
		MaxDeleteCountInTx:     10,
		MaxTxCountInEpoch:      100, // For rabbit, we cannot avoid inter-tx dependency prehand, but it seldom happens
		MaxEpochCountInBlock:   900,
		EffectiveBits:          0xFF000000_000003FF,
		MaxActiveCount:         128 * 1024,
		MaxValueLength:         256,
		TxSucceedRatio:         0.85,
		BlockSucceedRatio:      0.95,
		BlockPanicRatio:        0.03,
		RootType:               "Real", //"MockDataTree", //"MockRoot",
		ConsistencyEveryNBlock: 10,
		PruneEveryNBlock:       100,
		KeepRecentNBlock:       100,
		ReloadEveryNBlock:      500,
	}
	runTest(cfg2)

}

// ==============================
const (
	FirstByteOfCacheableKey = byte(105)
)

var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd   = []byte{255, 255, 255, 255, 255, 255, 255, 255}
)

var DBG bool

type FuzzConfig struct {
	MaxReadCountInTx       uint32
	MaxCheckProofCountInTx uint32
	MaxWriteCountInTx      uint32
	MaxDeleteCountInTx     uint32
	MaxTxCountInEpoch      uint32
	MaxEpochCountInBlock   uint32
	EffectiveBits          uint64
	MaxValueLength         int
	MaxActiveCount         int
	TxSucceedRatio         float32
	BlockSucceedRatio      float32
	BlockPanicRatio        float32
	RootType               string //MockRoot MockDataTree Real
	ConsistencyEveryNBlock int
	PruneEveryNBlock       int
	KeepRecentNBlock       int
	ReloadEveryNBlock      int
}

func runTest(cfg *FuzzConfig) {
	DBG = false
	randFilename := os.Getenv("RANDFILE")
	if len(randFilename) == 0 {
		fmt.Printf("No RANDFILE specified. Exiting...")
		return
	}
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		panic(err)
	}

	rs := randsrc.NewRandSrcFromFileWithSeed(randFilename, []byte{0})
	var root storetypes.RootStoreI
	var mads *moeingads.MoeingADS
	initRootAndMads := func() {
		var err error
		mads, err = moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
		if err != nil {
			panic(err)
		}
		root = store.NewRootStore(mads, func(k []byte) bool {
			return k[0] > FirstByteOfCacheableKey
		})
	}
	if cfg.RootType == "MockRoot" {
		root = store.NewMockRootStore()
	} else if cfg.RootType == "MockDataTree" {
		os.RemoveAll("./rocksdb.db")
		mads := moeingads.NewMoeingADS4Mock([][]byte{GuardStart, GuardEnd})
		root = store.NewRootStore(mads, func(k []byte) bool {
			return k[0] > FirstByteOfCacheableKey
		})
	} else if cfg.RootType == "Real" {
		os.RemoveAll("./moeingads4test")
		initRootAndMads()
	} else {
		panic("Invalid RootType " + cfg.RootType)
	}
	ref := NewRefL1()
	fmt.Printf("Initialized\n")

	block := GenerateRandBlock(0, ref, rs, cfg)
	madsHeight := 0
	for height := 0; height < roundCount; height++ {
		//if height > 66 {DBG = true}
		fmt.Printf("Block %d success %v\n", height, block.Succeed)
		//retry := ExecuteBlock(height, mads, root, block, cfg, false) //not in parallel
		retry := ExecuteBlock(height, mads, root, block, cfg, true) //in parallel
		if retry {
			initRootAndMads()
			if int64(madsHeight) != mads.GetCurrHeight() {
				panic(fmt.Sprintf("Height mismatch %d %d", madsHeight, mads.GetCurrHeight()))
			}
			height--
			continue // retry with the same height and same block
		}
		if block.Succeed && !retry {
			madsHeight = height
		}
		if block.Succeed && int64(height) != mads.GetCurrHeight() {
			panic(fmt.Sprintf("Height mismatch %d %d", height, mads.GetCurrHeight()))
		}
		if (height+1)%cfg.ReloadEveryNBlock == 0 {
			mads.Close()
			initRootAndMads()
		}
		block = GenerateRandBlock(height, ref, rs, cfg)
	}
	root.Close()
	if cfg.RootType == "MockDataTree" {
		os.RemoveAll("./rocksdb.db")
	} else if cfg.RootType == "Real" {
		os.RemoveAll("./moeingads4test")
	}
}

type Pair struct {
	Key, Value []byte
}

const (
	OpRead       = 8
	OpCheckProof = 4
	OpWrite      = 1
	OpDelete     = 0
)

type Operation struct {
	opType int
	key    [8]byte
	value  []byte
}

type Tx struct {
	OpList  []Operation
	Succeed bool
}

type Epoch struct {
	TxList []*Tx
}

type Block struct {
	EpochList   []Epoch
	Succeed     bool
	PanicNumber int
}

func getRandValue(rs randsrc.RandSrc, cfg *FuzzConfig) []byte {
	length := 1 + int(rs.GetUint32())%(cfg.MaxValueLength-1)     //no zero-length value
	if float32(rs.GetUint32()%0x10000)/float32(0x10000) < 0.15 { //some corner cases for large value
		length = 1 + int(rs.GetUint32())%(moeingads.HPFileBufferSize/2)
	}
	bz := rs.GetBytes(length)
	if len(bz) < 16 {
		return bz
	}
	for i := 0; i < 3; i++ {
		if float32(rs.GetUint32()%0x10000)/float32(0x10000) < 0.1 {
			continue
		}
		pos := int(rs.GetUint32()) % (length - 8)
		copy(bz[pos:], datatree.MagicBytes[:])
	}
	return bz
}

func getRand8Bytes(rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) (res [8]byte) {
	if touchedKeys == nil {
		i := rs.GetUint64() & cfg.EffectiveBits
		binary.LittleEndian.PutUint64(res[:], i)
		return
	}
	for {
		i := rs.GetUint64() & cfg.EffectiveBits
		if _, ok := touchedKeys[i]; ok {
			continue
		} else {
			binary.LittleEndian.PutUint64(res[:], i)
			break
		}
	}
	return
}

func GenerateRandTx(ref *RefL1, rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) *Tx {
	ref2 := NewRefL2(ref)
	var readCount, checkProofCount, writeCount, deleteCount uint32
	maxReadCount := rs.GetUint32() % (cfg.MaxReadCountInTx + 1)
	maxCheckProofCount := rs.GetUint32() % (cfg.MaxCheckProofCountInTx + 1)
	maxWriteCount := rs.GetUint32() % (cfg.MaxWriteCountInTx + 1)
	if cfg.MaxActiveCount > 0 && ref.Size() > cfg.MaxActiveCount {
		maxWriteCount = 0 //if we have too many active entries in ref store, stop writting
	}
	maxDeleteCount := rs.GetUint32() % (cfg.MaxDeleteCountInTx + 1)
	tx := Tx{
		OpList:  make([]Operation, 0, maxReadCount+maxWriteCount+maxDeleteCount),
		Succeed: float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.TxSucceedRatio,
	}
	for readCount != maxReadCount || checkProofCount != maxCheckProofCount ||
		writeCount != maxWriteCount || deleteCount != maxDeleteCount {
		if rs.GetUint32()%4 == 0 && readCount < maxReadCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			tx.OpList = append(tx.OpList, Operation{
				opType: OpRead,
				key:    key,
				value:  ref2.Get(key[:]),
			})
			readCount++
		}
		if rs.GetUint32()%4 == 1 && checkProofCount < maxCheckProofCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			tx.OpList = append(tx.OpList, Operation{
				opType: OpCheckProof,
				key:    key,
			})
			checkProofCount++
		}
		if rs.GetUint32()%4 == 2 && writeCount < maxWriteCount {
			op := Operation{
				opType: OpWrite,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
				value:  getRandValue(rs, cfg),
			}
			ref2.Set(op.key[:], op.value[:])
			tx.OpList = append(tx.OpList, op)
			writeCount++
		}
		if rs.GetUint32()%4 == 3 && deleteCount < maxDeleteCount {
			op := Operation{
				opType: OpDelete,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
			}
			ref2.Delete(op.key[:])
			tx.OpList = append(tx.OpList, op)
			deleteCount++
		}
	}
	if tx.Succeed { // to prevent inter-tx dependency
		for _, op := range tx.OpList {
			if op.opType == OpRead || op.opType == OpWrite || op.opType == OpDelete {
				touchedKeys[binary.LittleEndian.Uint64(op.key[:])] = struct{}{}
			}
		}
	}
	ref2.Close(tx.Succeed)
	return &tx
}

func GenerateRandEpoch(height, epochNum int, ref *RefL1, rs randsrc.RandSrc, cfg *FuzzConfig, blkSuc bool) Epoch {
	keyCountEstimated := cfg.MaxTxCountInEpoch * (cfg.MaxReadCountInTx +
		cfg.MaxWriteCountInTx + cfg.MaxDeleteCountInTx) / 2
	touchedKeys := make(map[uint64]struct{}, keyCountEstimated)
	txCount := rs.GetUint32() % (cfg.MaxTxCountInEpoch + 1)
	epoch := Epoch{TxList: make([]*Tx, int(txCount))}
	for i := range epoch.TxList {
		// Transactions in an epoch must be independent to each other. We use touchedKeys to ensure this
		tx := GenerateRandTx(ref, rs, cfg, touchedKeys)
		if DBG {
			fmt.Printf("FinishGeneration h:%d (%v) epoch %d tx %d (%v) of %d\n", height, blkSuc, epochNum, i, tx.Succeed, txCount)
			for j, op := range tx.OpList {
				fmt.Printf("See operation %d of %d\n", j, len(tx.OpList))
				fmt.Printf("%#v\n", op)
			}
		}
		epoch.TxList[i] = tx
	}

	return epoch
}

func GenerateRandBlock(height int, ref *RefL1, rs randsrc.RandSrc, cfg *FuzzConfig) *Block {
	epochCount := rs.GetUint32() % (cfg.MaxEpochCountInBlock + 1)
	block := &Block{EpochList: make([]Epoch, epochCount)}
	block.Succeed = float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.BlockSucceedRatio
	withPanic := float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.BlockPanicRatio
	if block.Succeed && withPanic {
		block.PanicNumber = int(1 + rs.GetUint32()%4)
	}
	for i := range block.EpochList {
		if DBG {
			fmt.Printf("Generating h:%d epoch %d of %d\n", height, i, epochCount)
		}
		block.EpochList[i] = GenerateRandEpoch(height, i, ref, rs, cfg, block.Succeed)
	}
	if block.Succeed {
		ref.FlushCache()
	}
	ref.ClearCache()
	return block
}

func MyGet(rbt rabbit.RabbitStore, key []byte) []byte {
	res := rbt.Get(key)
	if findIt := rbt.Has(key); findIt != (len(res) > 0) {
		panic(fmt.Sprintf("Bug in Has: %v %#v", findIt, res))
	}
	return res
}

func CheckTx(height, epochNum, txNum int, mads *moeingads.MoeingADS, rbt rabbit.RabbitStore, tx *Tx, cfg *FuzzConfig, blkSuc bool) {
	for i, op := range tx.OpList {
		if DBG {
			fmt.Printf("Check %d-%d (%v) tx %d (%v) operation %d of %d\n", height, epochNum, blkSuc, txNum, tx.Succeed, i, len(tx.OpList))
			fmt.Printf("%#v\n", op)
		}
		if op.opType == OpRead {
			bz := MyGet(rbt, op.key[:])
			if !bytes.Equal(op.value[:], bz) {
				panic(fmt.Sprintf("Error in Get %#v real %#v expected %#v", op.key[:], bz, op.value[:]))
			}
		} else if op.opType == OpCheckProof {
			path, ok := rbt.GetShortKeyPath(op.key[:])
			if ok && len(path) == 1 {
				entry := mads.GetEntry(path[0][:])
				if entry != nil {
					_, _, err := mads.GetProof(path[0][:])
					if err != nil {
						fmt.Printf("Why %#v err %#v\n", entry, err)
						panic(err)
					}
				}
			}
		} else if op.opType == OpWrite {
			rbt.Set(op.key[:], op.value[:])
		} else if op.opType == OpDelete {
			rbt.Delete(op.key[:])
		}
	}
}

func ExecuteBlock(height int, mads *moeingads.MoeingADS, root storetypes.RootStoreI, block *Block, cfg *FuzzConfig, inParallel bool) (retry bool) {
	root.SetHeight(int64(height))
	trunk := root.GetTrunkStore(1000).(*store.TrunkStore)
	for i, epoch := range block.EpochList {
		if DBG {
			fmt.Printf("Check h:%d (%v) epoch %d of %d\n", height, block.Succeed, i, len(block.EpochList))
		}
		dbList := make([]rabbit.RabbitStore, len(epoch.TxList))
		var wg sync.WaitGroup
		for j, tx := range epoch.TxList {
			dbList[j] = rabbit.NewRabbitStore(trunk)
			if DBG {
				fmt.Printf("Check h:%d (%v) epoch %d tx %d (%v) of %d\n", height, block.Succeed, i, j, tx.Succeed, len(epoch.TxList))
			}
			if inParallel {
				wg.Add(1)
				go func(tx *Tx, j int) {
					CheckTx(height, i, j, mads, dbList[j], tx, cfg, block.Succeed)
					wg.Done()
				}(tx, j)
			} else {
				CheckTx(height, i, j, mads, dbList[j], tx, cfg, block.Succeed)
			}

		}
		if inParallel {
			wg.Wait()
		}
		for j, tx := range epoch.TxList {
			if DBG {
				fmt.Printf("WriteBack %d-%d tx %d : %v\n", height, i, j, tx.Succeed)
			}
			dbList[j].Close()
			if tx.Succeed {
				dbList[j].WriteBack()
			}
		}
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("RECOVER %#v\n", err)
			moeingads.DebugPanicNumber = 0
			root.Close()
			retry = true
		}
	}()
	if block.PanicNumber != 0 {
		moeingads.DebugPanicNumber = block.PanicNumber
		block.PanicNumber = 0
	}
	trunk.Close(block.Succeed)
	if (height+1)%cfg.ConsistencyEveryNBlock == 0 {
		fmt.Printf("Now Check Consistency\n")
		mads.CheckConsistency()
		mads.CheckHashConsistency()
	}
	if (height+1)%cfg.PruneEveryNBlock == 0 && height > cfg.KeepRecentNBlock {
		mads.PruneBeforeHeight(int64(height - cfg.KeepRecentNBlock))
	}
	return false
}
