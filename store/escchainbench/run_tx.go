package main

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dterei/gotsc"
	"github.com/mmcloughlin/meow"
	//"github.com/pkg/profile"

	"github.com/smartbch/moeingads"
	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/store"
	"github.com/smartbch/moeingads/store/rabbit"
)

const (
	NumTxInEpoch    = 1024
	NumWorkers      = 128
	NumTxPerWorker  = NumTxInEpoch / NumWorkers
	NumEpochInBlock = 32
)

type Block struct {
	epochList [NumEpochInBlock]*Epoch
}

type Epoch struct {
	jobList [NumWorkers]*Job
	txList  [NumTxInEpoch]Tx
}

type AccountAndNum struct {
	acc *rabbit.CachedValue
	num [rabbit.KeySize]byte
}

type Job struct {
	changedAccList []AccountAndNum
}

func ReadEpoch(fin *os.File) *Epoch {
	epoch := &Epoch{}
	hash64 := meow.New64(0)
	for i := 0; i < NumWorkers; i++ {
		epoch.jobList[i] = &Job{}
		for j := 0; j < NumTxPerWorker; j++ {
			var bz [TxLen]byte
			_, err := fin.Read(bz[:])
			if err != nil {
				panic(err)
			}
			tx := ParseTx(bz)
			epoch.txList[i*NumTxPerWorker+j] = tx
			tx.UpdateHash64(hash64)
		}
		epoch.jobList[i].changedAccList = make([]AccountAndNum, 0, 2*NumTxPerWorker)
	}
	var buf [8]byte
	_, err := fin.Read(buf[:])
	if err != nil {
		panic(err)
	}
	if hash64.Sum64() != binary.LittleEndian.Uint64(buf[:]) {
		panic("Epoch checksum error!")
	}
	return epoch
}

func (epoch Epoch) Check() bool {
	touchedNum := make(map[[rabbit.KeySize]byte]struct{}, 2*NumTxInEpoch)
	for i := 0; i < NumTxInEpoch; i++ {
		tx := epoch.txList[i]
		_, fromConflict := touchedNum[tx.FromNum]
		_, toConflict := touchedNum[tx.ToNum]
		if fromConflict || toConflict {
			return false
		}
		touchedNum[tx.FromNum] = struct{}{}
		touchedNum[tx.ToNum] = struct{}{}
	}
	return true
}

func (epoch Epoch) Run(root *store.RootStore) {
	var wg sync.WaitGroup
	isValid := true
	wg.Add(1)
	go func() {
		isValid = epoch.Check()
		wg.Done()
	}()
	sharedIdx := int64(-1)
	datatree.ParallelRun(NumWorkersInBlock, func(jobID int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(epoch.txList)) {
				break
			}
			epoch.jobList[jobID].executeTx(root, epoch.txList[myIdx])
		}
	})
	wg.Wait()
	if !isValid {
		fmt.Printf("Found an invalid epoch!")
		return
	}
}

//func getShortKey(n uint64) []byte {
//	var shortKey [rabbit.KeySize]byte
//	binary.LittleEndian.PutUint64(shortKey[:], n)
//	return shortKey[:]
//}

func GetCachedValue(root *store.RootStore, key []byte) *rabbit.CachedValue {
	bz := root.Get(key)
	if len(bz) == 0 {
		fmt.Printf("Cannot find account %#v\n", key)
		return nil
	}
	return rabbit.BytesToCachedValue(bz)
}

func (job *Job) executeTx(root *store.RootStore, tx Tx) {
	var fromAcc, toAcc Account
	root.PrepareForUpdate(tx.FromNum[:])
	fromAccWrap := GetCachedValue(root, tx.FromNum[:])
	if fromAccWrap == nil {
		fmt.Printf("Cannot find from-account %#v\n", tx.FromNum)
		return
	}
	root.PrepareForUpdate(tx.ToNum[:])
	toAccWrap := GetCachedValue(root, tx.ToNum[:])
	if toAccWrap == nil {
		fmt.Printf("Cannot find to-account %#v\n", tx.ToNum)
		return
	}
	fromAcc.FromBytes(fromAccWrap.GetValue())
	toAcc.FromBytes(toAccWrap.GetValue())
	fromIdx := fromAcc.Find(tx.CoinID)
	if fromIdx < 0 {
		fmt.Printf("Cannot find the token in from-account\n")
		return
	}
	toIdx := toAcc.Find(tx.CoinID)
	if toIdx < 0 {
		fmt.Printf("Cannot find the token in to-account\n")
		return
	}
	amount := int64(binary.LittleEndian.Uint64(tx.Amount[:]))
	if amount < 0 {
		amount = -amount
	}
	nativeTokenAmount, fromAmount, toAmount, toNewAmount := &big.Int{}, &big.Int{}, &big.Int{}, &big.Int{}
	fromAmount.SetBytes(fromAcc.GetTokenAmount(fromIdx))
	toAmount.SetBytes(toAcc.GetTokenAmount(toIdx))
	amountInt := big.NewInt(amount)
	if fromAmount.Cmp(amountInt) < 0 { // not enough tokens
		fmt.Printf("Not enough token")
		return // fail
	}
	fromAmount.Sub(fromAmount, amountInt)
	toNewAmount.Add(toAmount, amountInt)
	if toNewAmount.Cmp(toAmount) < 0 { //overflow
		fmt.Printf("token overflow")
		return // fail
	}
	fromAcc.SetTokenAmount(fromIdx, BigIntToBytes(fromAmount))
	toAcc.SetTokenAmount(toIdx, BigIntToBytes(toAmount))
	nativeTokenAmount.SetBytes(fromAcc.GetNativeAmount())
	gas := big.NewInt(10)
	if nativeTokenAmount.Cmp(gas) < 0 { //overflow
		fmt.Printf("not enough native token for gas")
		return // fail
	}
	nativeTokenAmount.Sub(nativeTokenAmount, gas)
	fromAcc.SetNativeAmount(BigIntToBytes(nativeTokenAmount))
	fromAcc.SetSequence(fromAcc.GetSequence() + 1)
	fromAccWrap.SetValue(fromAcc.ToBytes())
	toAccWrap.SetValue(toAcc.ToBytes())
	job.changedAccList = append(job.changedAccList, AccountAndNum{fromAccWrap, tx.FromNum})
	job.changedAccList = append(job.changedAccList, AccountAndNum{toAccWrap, tx.ToNum})
}

func RunTx(numBlock int, txFile string) {
	tscOverhead = gotsc.TSCOverhead()

	fin, err := os.Open(txFile)
	if err != nil {
		panic(err)
	}
	defer fin.Close()
	ch := make(chan *Block, 2)
	go func() {
		for i := 0; i < numBlock; i++ {
			var block Block
			for j := 0; j < NumEpochInBlock; j++ {
				block.epochList[j] = ReadEpoch(fin)
			}
			ch <- &block
		}
	}()

	fmt.Printf("Start %f\n", float64(time.Now().UnixNano())/1e9)
	mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %f\n", float64(time.Now().UnixNano())/1e9)
	startHeight := mads.GetCurrHeight() + 1
	root := store.NewRootStore(mads, nil)
	//pjob := profile.Start()
	for i := 0; i < numBlock; i++ {
		//start := gotsc.BenchStart()
		height := startHeight + int64(i)
		fmt.Printf("block %d (%d)\n", i, height)
		root.SetHeight(height)
		block := <-ch
		for j := 0; j < NumEpochInBlock; j++ {
			block.epochList[j].Run(root)
		}
		//Phase1Time += gotsc.BenchEnd() - start - tscOverhead
		//start = gotsc.BenchStart()
		root.BeginWrite()
		for j := 0; j < NumEpochInBlock; j++ {
			for k := 0; k < NumWorkers; k++ {
				for _, accAndNum := range block.epochList[j].jobList[k].changedAccList {
					root.Set(accAndNum.num[:], accAndNum.acc.ToBytes())
				}
			}
		}
		root.EndWrite()
		//Phase2Time += gotsc.BenchEnd() - start - tscOverhead
		if height > 100 && height%100 == 0 {
			mads.PruneBeforeHeight(height - 100)
		}
	}
	//pjob.Stop()
	fmt.Printf("Finished %f\n", float64(time.Now().UnixNano())/1e9)
	//fmt.Printf("phase1 time %d\n", Phase1Time)
	//fmt.Printf("phase2 time %d\n", Phase2Time)
	//fmt.Printf("moeingads.phase0 time %d\n", moeingads.Phase0Time)
	//fmt.Printf("moeingads.pha1n2 time %d\n", moeingads.Phase1n2Time)
	//fmt.Printf("moeingads.phase1 time %d\n", moeingads.Phase1Time)
	//fmt.Printf("moeingads.phase2 time %d\n", moeingads.Phase2Time)
	//fmt.Printf("moeingads.phase3 time %d\n", moeingads.Phase3Time)
	//fmt.Printf("moeingads.phase4 time %d\n", moeingads.Phase4Time)
	//mads.PrintMetaInfo()
	root.Close()
}

/*
phase1 time 659216297850
phase2 time 492814142134
moeingads.phase0 time 0
moeingads.pha1n2 time 244571442010
moeingads.phase1 time 0
moeingads.phase2 time 0
moeingads.phase3 time 49584349112
moeingads.phase4 time 143403222792

Finished 1594699845.963070
phase1 time 1950429877590
phase2 time 1731343352988
moeingads.phase0 time 0
moeingads.pha1n2 time 829604727502
moeingads.phase1 time 0
moeingads.phase2 time 0
moeingads.phase3 time 328017075308
moeingads.phase4 time 399895217040
*/
