package rabbit

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/coinexchain/randsrc"

	"github.com/moeing-chain/MoeingADS/store"
)

// go test -c -coverpkg github.com/moeing-chain/MoeingADS/store/rabbit .

// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=10000 ./rabbit.test

const KeyLen = 16
const ValueLen = 9

type FuzzConfig struct {
	MaxSize           int
	MinSize           int
	RunSteps          int
	CompareInterval   int
	WritebackInterval int
}

func FuzzSet(cfg *FuzzConfig, rs randsrc.RandSrc, refMap map[[KeyLen]byte][]byte, rbt RabbitStore) {
	var key [KeyLen]byte
	for i := 0; i < cfg.RunSteps; i++ {
		copy(key[:], rs.GetBytes(KeyLen))
		value := rs.GetBytes(ValueLen)
		refMap[key] = value
		rbt.Set(key[:], value)
	}
}

func FuzzModify(cfg *FuzzConfig, rs randsrc.RandSrc, refMap map[[KeyLen]byte][]byte, rbt RabbitStore) {
	finishedSteps := 0
	skippedEntries := 0
	toBeSkipped := int(rs.GetUint64())%len(refMap) - cfg.RunSteps
	if toBeSkipped < 0 {
		toBeSkipped = 0
	}

	for k, v := range refMap {
		if skippedEntries < toBeSkipped {
			skippedEntries++
			continue
		}
		rV := rbt.Get(k[:])
		if rV == nil || !bytes.Equal(rV, v) {
			fmt.Printf("k:%v rV:%v v:%v\n", k[:], rV, v)
			panic("Compare Error")
		}
		switch rs.GetUint64() % 3 {
		case 0: //delete
			rbt.Delete(k[:])
			delete(refMap, k)
			ok := rbt.Has(k[:])
			if ok {
				fmt.Printf("The deleted entry %v\n", k[:])
				panic("Why? it was deleted...")
			}
		case 1: //change
			newV := rs.GetBytes(ValueLen)
			refMap[k] = newV
			rbt.Set(k[:], newV)
		case 2: //delete and change
			rbt.Delete(k[:])
			delete(refMap, k)
			ok := rbt.Has(k[:])
			if ok {
				fmt.Printf("The deleted entry %v\n", k[:])
				panic("Why? it was deleted...")
			}
			newV := rs.GetBytes(ValueLen)
			refMap[k] = newV
			rbt.Set(k[:], newV)
		}
		finishedSteps++
		if finishedSteps >= cfg.RunSteps {
			break
		}
	}
}

func RunCompare(refMap map[[KeyLen]byte][]byte, rbt RabbitStore) {
	for k, v := range refMap {
		rV := rbt.Get(k[:])
		if rV == nil || !bytes.Equal(rV, v) {
			fmt.Printf("rV:%v v:%v\n", rV, v)
			panic("Compare Error")
		}
	}
}

func RunFuzz(cfg *FuzzConfig, roundCount int, randFilename string) {
	if KeySize != 2 {
		panic("KeySize != 2")
	}
	rs := randsrc.NewRandSrcFromFile(randFilename)
	refMap := make(map[[KeyLen]byte][]byte, cfg.MaxSize)
	root := store.NewMockRootStore()
	trunk := root.GetTrunkStore().(*store.TrunkStore)
	rbt := NewRabbitStore(trunk)
	for i := 0; i < roundCount; i++ {
		if i%100 == 0 {
			fmt.Printf("now round %d %f\n", i, float64(len(refMap))/float64(rbt.ActiveCount()))
		}
		if len(refMap) < cfg.MaxSize {
			//fmt.Printf("FuzzSet\n")
			FuzzSet(cfg, rs, refMap, rbt)
		}
		if len(refMap) > cfg.MinSize {
			//fmt.Printf("FuzzModify\n")
			FuzzModify(cfg, rs, refMap, rbt)
		}
		if i%cfg.CompareInterval == 0 {
			//fmt.Printf("RunCompare\n")
			RunCompare(refMap, rbt)
		}
		if i%cfg.WritebackInterval == 0 {
			rbt.Close()
			trunk.Close(true)
			trunk = root.GetTrunkStore().(*store.TrunkStore)
			rbt = NewRabbitStore(trunk)
		}
	}
}

func runTest(cfg *FuzzConfig) {
	randFilename := os.Getenv("RANDFILE")
	if len(randFilename) == 0 {
		fmt.Printf("No RANDFILE specified. Exiting...")
		return
	}
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		panic(err)
	}

	RunFuzz(cfg, roundCount, randFilename)
}

func Test1(t *testing.T) {
	cfg := &FuzzConfig{
		MaxSize:           256 * 256 / 16,
		MinSize:           256 * 256 / 32,
		RunSteps:          100,
		CompareInterval:   1000,
		WritebackInterval: 3000,
	}

	//cfg = &FuzzConfig{
	//	MaxSize:         256*256/128,
	//	MinSize:         256*256/256,
	//	RunSteps:        100,
	//	CompareInterval: 1000,
	//	WritebackInterval: 3000,
	//}

	runTest(cfg)
}
