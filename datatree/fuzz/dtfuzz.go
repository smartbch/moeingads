package fuzz

import (
	"fmt"
	"os"
	"strconv"

	"github.com/coinexchain/randsrc"

	"github.com/moeing-chain/MoeingADS/datatree"
)

const (
	PruneRatio = 0.5
)

// go test -c . -coverpkg=github.com/moeing-chain/MoeingADS/datatree
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=$((120*10000)) ./fuzz.test -test.coverprofile system.out

func runTest() {
	randFilename := os.Getenv("RANDFILE")
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		panic(err)
	}

	rs := randsrc.NewRandSrcFromFileWithSeed(randFilename, []byte{0})
	ctx := NewContext(DefaultConfig, rs)
	ctx.initialAppends()
	fmt.Printf("Initialized\n")
	for i := 0; i < roundCount; i++ {
		if i%10000 == 0 {
			fmt.Printf("Now %d of %d activeCount %d of %d\n", i, roundCount, ctx.activeCount, ctx.cfg.MaxActiveCount)
		}
		ctx.step()
	}
}

type FuzzConfig struct {
	EndBlockStripe         uint32 // run EndBlock every n steps
	ConsistencyEveryNBlock uint32 // check consistency every n blocks
	ReloadEveryNBlock      uint32 // reload tree from disk every n blocks
	RecoverEveryNBlock     uint32 // recover tree from disk every n blocks
	PruneEveryNBlock       uint32 // prune the tree every n blocks
	MaxKVLen               uint32 // max length of key and value
	DeactiveStripe         uint32 // deactive some entry every n steps
	DeactiveCount          uint32 // number of deactive try times
	MassDeactiveStripe     uint32 // deactive many entries every n steps
	ProofCount             uint32 // check several proofs at endblock
	MaxActiveCount         uint32 // the maximum count of active entries
	MagicBytesInKey        uint32 // chance that keys have magicbytes
	MagicBytesInValue      uint32 // chance that value have magicbytes
	PruneToOldestMaxDist   uint32 // the maximum possible value of oldestActiveTwigID-lastPrunedTwigID
}

var DefaultConfig = FuzzConfig{
	EndBlockStripe:         1000,
	ConsistencyEveryNBlock: 20,
	ReloadEveryNBlock:      309,
	RecoverEveryNBlock:     606,
	PruneEveryNBlock:       20,
	MaxKVLen:               20,
	DeactiveStripe:         3,
	DeactiveCount:          4,
	MassDeactiveStripe:     6000,
	ProofCount:             4,
	MaxActiveCount:         1 * 1024 * 1024,
	MagicBytesInKey:        1000,
	MagicBytesInValue:      2000,
	PruneToOldestMaxDist:   15,
}

type Context struct {
	tree      *datatree.Tree
	rs        randsrc.RandSrc
	cfg       FuzzConfig
	edgeNodes []*datatree.EdgeNode

	oldestActiveTwigID int64
	serialNum          int64
	lastPrunedTwigID   int64
	activeCount        int64
	height             int64
	stepCount          int64
}

const (
	defaultFileSize = 16 * 1024 * 1024
	dirName         = "./datadir"
)

func NewContext(cfg FuzzConfig, rs randsrc.RandSrc) *Context {
	os.RemoveAll(dirName)
	os.Mkdir(dirName, 0700)
	return &Context{
		tree: datatree.NewEmptyTree(datatree.BufferSize, defaultFileSize, dirName),
		rs:   rs,
		cfg:  cfg,
	}
}

func (ctx *Context) oldestActiveSN() int64 {
	return ctx.oldestActiveTwigID * datatree.LeafCountInTwig
}

func (ctx *Context) generateRandSN() int64 {
	oldest := ctx.oldestActiveSN()
	return oldest + int64(ctx.rs.GetUint64()%uint64(ctx.serialNum-oldest))
}

func (ctx *Context) oldestInactiveSN() int64 {
	return (ctx.lastPrunedTwigID + 1) * datatree.LeafCountInTwig
}

func (ctx *Context) generateRandSN4Proof() int64 {
	oldest := ctx.oldestInactiveSN()
	return oldest + int64(ctx.rs.GetUint64()%uint64(ctx.serialNum-oldest))
}

func (ctx *Context) generateRandEntry() *datatree.Entry {
	e := &datatree.Entry{
		Key:        ctx.rs.GetBytes(int(ctx.rs.GetUint32() % ctx.cfg.MaxKVLen)),
		Value:      ctx.rs.GetBytes(int(ctx.rs.GetUint32() % ctx.cfg.MaxKVLen)),
		NextKey:    ctx.rs.GetBytes(int(ctx.rs.GetUint32() % ctx.cfg.MaxKVLen)),
		Height:     ctx.height,
		LastHeight: 0,
		SerialNum:  ctx.serialNum,
	}
	if ctx.rs.GetUint32()%ctx.cfg.MagicBytesInKey == 0 && len(e.Key) > 8 {
		pos := int(ctx.rs.GetUint32()) % (len(e.Key) - 8)
		copy(e.Key[pos:], datatree.MagicBytes[:])
	}
	if ctx.rs.GetUint32()%ctx.cfg.MagicBytesInValue == 0 && len(e.Value) > 8 {
		pos := int(ctx.rs.GetUint32()) % (len(e.Value) - 8)
		copy(e.Value[pos:], datatree.MagicBytes[:])
	}
	ctx.serialNum++
	return e
}

func (ctx *Context) initialAppends() {
	ctx.activeCount = int64(ctx.cfg.MaxActiveCount / 2)
	for i := int64(0); i < ctx.activeCount; i++ {
		entry := ctx.generateRandEntry()
		ctx.tree.AppendEntry(entry)
	}
}

func (ctx *Context) step() {
	if ctx.rs.GetUint32()%ctx.cfg.DeactiveStripe == 0 {
		for i := 0; i < int(ctx.cfg.DeactiveCount); i++ {
			sn := ctx.generateRandSN()
			//if datatree.Debug {
			//	fmt.Printf("Try to deactive %d %v\n", sn, ctx.tree.GetActiveBit(sn))
			//}
			if ctx.tree.GetActiveBit(sn) {
				ctx.tree.DeactiviateEntry(sn)
				ctx.activeCount--
			}
		}
	}
	if ctx.rs.GetUint32()%ctx.cfg.MassDeactiveStripe == 0 {
		fmt.Printf("Now MassDeactive\n")
		for i := 0; i < 4*datatree.DeactivedSNListMaxLen; i++ {
			sn := ctx.generateRandSN()
			if ctx.tree.GetActiveBit(sn) {
				ctx.tree.DeactiviateEntry(sn)
				ctx.activeCount--
			}
		}
	}
	if ctx.activeCount < int64(ctx.cfg.MaxActiveCount) {
		entry := ctx.generateRandEntry()
		ctx.tree.AppendEntry(entry) // make sure every Deactivation is followed by AppendEntry
		ctx.activeCount++
	}
	if ctx.rs.GetUint32()%ctx.cfg.EndBlockStripe == 0 {
		ctx.endBlock()
	}
	//if ctx.stepCount >= 420000 {
	//	datatree.Debug = true
	//}
	ctx.stepCount++
}

func (ctx *Context) endBlock() {
	ctx.height++
	//fmt.Printf("Now EndBlock %d\n", ctx.stepCount)
	ctx.tree.EndBlock()
	for i := 0; i < int(ctx.cfg.ProofCount); i++ {
		sn := ctx.oldestInactiveSN()
		if i > 0 {
			sn = ctx.generateRandSN4Proof()
		}
		//fmt.Printf("oldestInactiveSN %d lastPrunedTwigID %d oldestActiveTwigID %d serialNum %d sn %d\n",
		//	ctx.oldestInactiveSN(), ctx.lastPrunedTwigID, ctx.oldestActiveTwigID, ctx.serialNum, sn)
		path := ctx.tree.GetProof(sn)
		err := path.Check(false)
		if err != nil {
			panic(err)
		}
		bz := path.ToBytes()
		path2, err := datatree.BytesToProofPath(bz)
		if err != nil {
			panic(err)
		}
		err = path2.Check(true)
		if err != nil {
			panic(err)
		}
	}
	if ctx.height%int64(ctx.cfg.ConsistencyEveryNBlock) == 0 {
		fmt.Printf("Now CheckHashConsistency\n")
		datatree.CheckHashConsistency(ctx.tree)
	}
	if ctx.height%int64(ctx.cfg.ReloadEveryNBlock) == 0 {
		fmt.Printf("Now reloadTree\n")
		ctx.reloadTree()
	}
	if (ctx.height%int64(ctx.cfg.RecoverEveryNBlock) == 0) && (ctx.stepCount > 0*1320*10000) {
		fmt.Printf("Now recoverTree stepCount=%d\n", ctx.stepCount)
		fmt.Printf("recoverTree edgeNodes %#v\n", ctx.edgeNodes)
		//if len(ctx.edgeNodes) != 0 {ctx.recoverTree()}
		ctx.recoverTree()
	}
	if ctx.height%int64(ctx.cfg.PruneEveryNBlock) == 0 {
		ctx.pruneTree()
	}
}

func (ctx *Context) reloadTree() {
	ctx.tree.Flush()
	tree1 := datatree.LoadTree(datatree.BufferSize, defaultFileSize, dirName)

	datatree.CompareTreeTwigs(ctx.tree, tree1)
	datatree.CompareTreeNodes(ctx.tree, tree1)
	datatree.CheckHashConsistency(tree1)
	ctx.tree.Close()
	ctx.tree = tree1
}

func (ctx *Context) recoverTree() {
	ctx.tree.Flush()
	tree1 := datatree.RecoverTree(datatree.BufferSize, defaultFileSize, dirName,
		ctx.edgeNodes, ctx.lastPrunedTwigID, ctx.oldestActiveTwigID, ctx.serialNum>>datatree.TwigShift)

	datatree.CompareTreeTwigs(ctx.tree, tree1)
	datatree.CheckHashConsistency(tree1)
	ctx.tree.Close()
	ctx.tree = tree1
}

func (ctx *Context) getRatio() float64 {
	return float64(ctx.activeCount) / float64(ctx.serialNum-ctx.oldestActiveSN())
}

//!! func (ctx *Context) pruneTree() {
//!! 	fmt.Printf("Try pruneTree %f %d %d\n", ctx.getRatio(), ctx.activeCount, ctx.serialNum - ctx.oldestActiveSN())
//!! 	for ctx.getRatio() < PruneRatio {
//!! 		entryChan := ctx.tree.GetActiveEntriesInTwigOld(ctx.oldestActiveTwigID)
//!! 		var aList, bList [][]byte
//!! 		serialNum2 := ctx.serialNum
//!!
//!! 		entryBzChan := ctx.tree.GetActiveEntriesInTwig(ctx.oldestActiveTwigID)
//!! 		for entryBz := range entryBzChan {
//!! 			datatree.UpdateSerialNum(entryBz, serialNum2)
//!! 			bList = append(bList, datatree.ExtractKeyFromRawBytes(entryBz))
//!! 			serialNum2++
//!! 		}
//!!
//!! 		for entry := range entryChan {
//!! 			sn := entry.SerialNum
//!! 			if sn < 0 || sn > (1<<31) {
//!! 				fmt.Printf("Why? sn=%d\n", sn)
//!! 			}
//!! 			if ctx.tree.GetActiveBit(sn) {
//!! 				ctx.tree.DeactiviateEntry(sn)
//!! 				//entry.SerialNum = ctx.serialNum
//!! 				//ctx.tree.AppendEntry(entry)
//!! 				bz := datatree.EntryToBytes(*entry, nil)
//!! 				aList = append(aList, entry.Key)
//!! 				datatree.UpdateSerialNum(bz, ctx.serialNum)
//!! 				ctx.tree.AppendEntryRawBytes(bz, ctx.serialNum)
//!! 				ctx.serialNum++
//!! 			}
//!! 		}
//!!
//!! 		if len(aList) != len(bList) {
//!! 			fmt.Printf("%d vs %d\n", len(aList), len(bList))
//!! 			panic("different length")
//!! 		}
//!! 		for i := range aList {
//!! 			if !bytes.Equal(aList[i][:], bList[i][:]) {
//!! 				fmt.Printf("a %#v\n", aList[i])
//!! 				fmt.Printf("b %#v\n", bList[i])
//!! 				panic("different content")
//!! 			}
//!! 		}
//!!
//!! 		ctx.tree.EvictTwig(ctx.oldestActiveTwigID)
//!! 		ctx.oldestActiveTwigID++
//!! 	}
//!! 	ctx.tree.EndBlock()
//!! 	fmt.Printf("Now oldestActiveTwigID %d serialNum %d\n", ctx.oldestActiveTwigID, ctx.serialNum)
//!! 	endID := ctx.oldestActiveTwigID - 1 - int64(ctx.rs.GetUint32()%ctx.cfg.PruneToOldestMaxDist)
//!! 	fmt.Printf("Now pruneTree(%f) %d %d\n", ctx.getRatio(), ctx.lastPrunedTwigID, endID)
//!! 	if endID - ctx.lastPrunedTwigID >= datatree.MinPruneCount {
//!! 		fmt.Printf("Now run PruneTwigs %d %d oldestActiveTwigID %d\n", ctx.lastPrunedTwigID, endID, ctx.oldestActiveTwigID)
//!! 		bz := ctx.tree.PruneTwigs(ctx.lastPrunedTwigID, endID)
//!! 		ctx.edgeNodes = datatree.BytesToEdgeNodes(bz)
//!! 		fmt.Printf("Here the edgeNodes %v\n", ctx.edgeNodes)
//!! 		ctx.lastPrunedTwigID = endID
//!! 	}
//!! }

func (ctx *Context) pruneTree() {
	fmt.Printf("Try pruneTree %f %d %d\n", ctx.getRatio(), ctx.activeCount, ctx.serialNum-ctx.oldestActiveSN())
	for ctx.getRatio() < PruneRatio {
		entryChan := ctx.tree.GetActiveEntriesInTwig(ctx.oldestActiveTwigID)
		for entryBz := range entryChan {
			sn := datatree.ExtractSerialNum(entryBz)
			if sn < 0 || sn > (1<<31) {
				fmt.Printf("Why? sn=%d\n", sn)
			}
			if ctx.tree.GetActiveBit(sn) {
				ctx.tree.DeactiviateEntry(sn)
				//!! fmt.Printf("Fuck0 %#v\n", entryBz)
				datatree.UpdateSerialNum(entryBz, ctx.serialNum)
				//!! fmt.Printf("Fuck1 %#v\n", entryBz)
				ctx.tree.AppendEntryRawBytes(entryBz, ctx.serialNum)
				ctx.serialNum++
			}
		}
		ctx.tree.EvictTwig(ctx.oldestActiveTwigID)
		ctx.oldestActiveTwigID++
	}
	ctx.tree.EndBlock()
	fmt.Printf("Now oldestActiveTwigID %d serialNum %d\n", ctx.oldestActiveTwigID, ctx.serialNum)
	endID := ctx.oldestActiveTwigID - 1 - int64(ctx.rs.GetUint32()%ctx.cfg.PruneToOldestMaxDist)
	fmt.Printf("Now pruneTree(%f) %d %d\n", ctx.getRatio(), ctx.lastPrunedTwigID, endID)
	if endID-ctx.lastPrunedTwigID >= datatree.MinPruneCount {
		fmt.Printf("Now run PruneTwigs %d %d oldestActiveTwigID %d\n", ctx.lastPrunedTwigID, endID, ctx.oldestActiveTwigID)
		bz := ctx.tree.PruneTwigs(ctx.lastPrunedTwigID, endID)
		ctx.edgeNodes = datatree.BytesToEdgeNodes(bz)
		fmt.Printf("Here the edgeNodes %v\n", ctx.edgeNodes)
		ctx.lastPrunedTwigID = endID
	}
}
