package fuzz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/coinexchain/randsrc"

	moeingads "github.com/moeing-chain/MoeingADS"
	"github.com/moeing-chain/MoeingADS/store"
	"github.com/moeing-chain/MoeingADS/store/rabbit"
	storetypes "github.com/moeing-chain/MoeingADS/store/types"
)

const (
	FirstByteOfCacheableKey = byte(15)
)

var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd   = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	EndKey     = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255}
)

var DBG bool

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
	if cfg.RootType == "MockRoot" {
		root = store.NewMockRootStore()
	} else if cfg.RootType == "MockDataTree" {
		os.RemoveAll("./rocksdb.db")
		mads := moeingads.NewMoeingADS4Mock([][]byte{GuardStart, GuardEnd})
		root = store.NewRootStore(mads, func(k []byte) bool {
			return (k[0] & FirstByteOfCacheableKey) == FirstByteOfCacheableKey
		})
	} else if cfg.RootType == "Real" {
		os.RemoveAll("./moeingads4test")
		mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
		if err != nil {
			panic(err)
		}
		root = store.NewRootStore(mads, func(k []byte) bool {
			return (k[0] & FirstByteOfCacheableKey) == FirstByteOfCacheableKey
		})
	} else {
		panic("Invalid RootType " + cfg.RootType)
	}
	ref := NewRefStore()
	fmt.Printf("Initialized\n")

	for i := 0; i < roundCount; i++ {
		//if i > 66 {DBG = true}
		fmt.Printf("Block %d\n", i)
		root.CheckConsistency()
		block := GenerateRandBlock(i, ref, rs, cfg)
		//ExecuteBlock(i, root, &block, rs, cfg, false) //not in parallel
		ExecuteBlock(i, root, &block, rs, cfg, true) //in parallel
	}
	root.Close()
	if cfg.RootType == "MockDataTree" {
		os.RemoveAll("./rocksdb.db")
	} else if cfg.RootType == "Real" {
		os.RemoveAll("./moeingads4test")
	}
}

const (
	OpRead   = 8
	OpWrite  = 1
	OpDelete = 0
)

type Coord struct {
	x, y uint32
}

func (coord *Coord) ToBytes() []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[:4], coord.x)
	binary.LittleEndian.PutUint32(buf[4:], coord.y)
	return buf[:]
}

func (coord *Coord) FromBytes(buf []byte) {
	if len(buf) != 8 {
		panic(fmt.Sprintf("length is not 8: %#v", buf))
	}
	coord.x = binary.LittleEndian.Uint32(buf[:4])
	coord.y = binary.LittleEndian.Uint32(buf[4:])
}

func (coord *Coord) DeepCopy() interface{} {
	return &Coord{
		x: coord.x,
		y: coord.y,
	}
}

type FuzzConfig struct {
	MaxReadCountInTx     uint32
	MaxWriteCountInTx    uint32
	MaxDeleteCountInTx   uint32
	MaxTxCountInEpoch    uint32
	MaxEpochCountInBlock uint32
	EffectiveBits        uint32
	MaxActiveCount       int
	TxSucceedRatio       float32
	BlockSucceedRatio    float32
	RootType             string //MockRoot MockDataTree Real
}

type Pair struct {
	Key, Value []byte
}

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
	EpochList []Epoch
	Succeed   bool
}

func getRand8Bytes(rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) (res [8]byte) {
	sh := 62 - cfg.EffectiveBits
	if touchedKeys == nil {
		i := rs.GetUint64()
		i = ((i << sh) >> sh) | 3
		binary.LittleEndian.PutUint64(res[:], i)
		return
	}
	for {
		i := rs.GetUint64()
		i = ((i << sh) >> sh) | 3
		if _, ok := touchedKeys[i]; ok {
			continue
		} else {
			binary.LittleEndian.PutUint64(res[:], i)
			break
		}
	}
	return
}

func GenerateRandTx(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) *Tx {
	readCount, writeCount, deleteCount := uint32(0), uint32(0), uint32(0)
	maxReadCount := rs.GetUint32() % (cfg.MaxReadCountInTx + 1)
	maxWriteCount := rs.GetUint32() % (cfg.MaxWriteCountInTx + 1)
	if cfg.MaxActiveCount > 0 && ref.Size() > cfg.MaxActiveCount {
		maxWriteCount = 0
	}
	maxDeleteCount := rs.GetUint32() % (cfg.MaxDeleteCountInTx + 1)
	tx := Tx{
		OpList:  make([]Operation, 0, maxReadCount+maxWriteCount+maxDeleteCount),
		Succeed: float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.TxSucceedRatio,
	}
	var undoList []UndoOp
	if !tx.Succeed {
		undoList = make([]UndoOp, 0, maxWriteCount+maxDeleteCount)
	}
	for readCount != maxReadCount || writeCount != maxWriteCount || deleteCount != maxDeleteCount {
		if rs.GetUint32()%4 == 0 && readCount < maxReadCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			tx.OpList = append(tx.OpList, Operation{
				opType: OpRead,
				key:    key,
				value:  ref.Get(key[:]),
			})
			readCount++
		}
		if rs.GetUint32()%4 == 0 && writeCount < maxWriteCount {
			v := getRand8Bytes(rs, cfg, nil)
			op := Operation{
				opType: OpWrite,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
				value:  v[:],
			}
			undo := ref.Set(op.key[:], op.value[:])
			if !tx.Succeed {
				undoList = append(undoList, undo)
			}
			tx.OpList = append(tx.OpList, op)
			writeCount++
		}
		if rs.GetUint32()%4 == 0 && deleteCount < maxDeleteCount {
			op := Operation{
				opType: OpDelete,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
			}
			undo := ref.Delete(op.key[:])
			if !tx.Succeed {
				undoList = append(undoList, undo)
			}
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
	} else { // to recover old state
		for i := len(undoList) - 1; i >= 0; i-- {
			undoOp := undoList[i]
			if undoOp.oldStatus == storetypes.Missed {
				ref.RealDelete(undoOp.key)
			} else if undoOp.oldStatus == storetypes.JustDeleted {
				ref.Delete(undoOp.key)
			} else {
				ref.RealSet(undoOp.key, undoOp.value)
			}
		}
	}
	return &tx
}

func GenerateRandEpoch(height, epochNum int, ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig, blkSuc bool) Epoch {
	keyCountEstimated := cfg.MaxTxCountInEpoch * (cfg.MaxReadCountInTx +
		cfg.MaxWriteCountInTx + cfg.MaxDeleteCountInTx) / 2
	touchedKeys := make(map[uint64]struct{}, keyCountEstimated)
	txCount := rs.GetUint32() % (cfg.MaxTxCountInEpoch + 1)
	epoch := Epoch{TxList: make([]*Tx, int(txCount))}
	for i := range epoch.TxList {
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

func GenerateRandBlock(height int, ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig) Block {
	epochCount := rs.GetUint32() % (cfg.MaxEpochCountInBlock + 1)
	block := Block{EpochList: make([]Epoch, epochCount)}
	block.Succeed = float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.BlockSucceedRatio
	refUsed := ref
	if !block.Succeed {
		refUsed = ref.Clone()
	}
	for i := range block.EpochList {
		if DBG {
			fmt.Printf("Generating h:%d epoch %d of %d\n", height, i, epochCount)
		}
		block.EpochList[i] = GenerateRandEpoch(height, i, refUsed, rs, cfg, block.Succeed)
	}
	return block
}

// TODO
func MyGet(multi rabbit.RabbitStore, key []byte) []byte {
	res := multi.Get(key)
	if multi.Has(key) != (len(res) > 0) {
		panic("Bug in Has")
	}
	return res
}

func CheckTx(height, epochNum, txNum int, multi rabbit.RabbitStore, tx *Tx, rs randsrc.RandSrc, cfg *FuzzConfig, blkSuc bool) {
	for i, op := range tx.OpList {
		if DBG {
			fmt.Printf("Check %d-%d (%v) tx %d (%v) operation %d of %d\n", height, epochNum, blkSuc, txNum, tx.Succeed, i, len(tx.OpList))
			fmt.Printf("%#v\n", op)
		}
		if op.opType == OpRead {
			bz := MyGet(multi, op.key[:])
			if !bytes.Equal(op.value[:], bz) {
				panic(fmt.Sprintf("Error in Get %#v real %#v expected %#v", op.key[:], bz, op.value[:]))
			}
		}
		if op.opType == OpWrite {
			multi.Set(op.key[:], op.value[:])
		}
		if op.opType == OpDelete {
			multi.Delete(op.key[:])
		}
	}
}

func ExecuteBlock(height int, root storetypes.RootStoreI, block *Block, rs randsrc.RandSrc, cfg *FuzzConfig, inParallel bool) {
	root.SetHeight(int64(height))
	trunk := root.GetTrunkStore().(*store.TrunkStore)
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
					CheckTx(height, i, j, dbList[j], tx, rs, cfg, block.Succeed)
					wg.Done()
				}(tx, j)
			} else {
				CheckTx(height, i, j, dbList[j], tx, rs, cfg, block.Succeed)
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
	trunk.Close(block.Succeed)
}
