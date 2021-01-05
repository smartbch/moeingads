package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/coinexchain/randsrc"

	cb "github.com/moeing-chain/MoeingADS/indextree/b/cppbtree"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <round-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	roundCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	RunFuzz(roundCount, DefaultConfig, randFilename)
}

var DefaultConfig = FuzzConfig{
	MaxInitCount: 200,
	ChangeCount:  100,
	QueryCount:   300,
	IterCount:    100,
	IterDistance: 20,
	MaxDelCount:  200,
	MinLen:       500,
	MaxLen:       1000,
}

type FuzzConfig struct {
	MaxInitCount int
	ChangeCount  int
	QueryCount   int
	IterCount    int
	IterDistance int
	MaxDelCount  int
	MinLen       int
	MaxLen       int
}

func assert(b bool, s string) {
	if !b {
		panic(s)
	}
}

const (
	Mask48 = (int64(1)<<48) - 1
)

func RunFuzz(roundCount int, cfg FuzzConfig, fname string) {
	rs := randsrc.NewRandSrcFromFile(fname)
	gobt := TreeNew()
	defer gobt.Close()
	cppbt := cb.TreeNew()
	defer cppbt.Close()
	addedKeyList := make([]uint64, 0, cfg.MaxInitCount)
	for i := 0; i < roundCount; i++ {
		if i%1000 == 0 {
			fmt.Printf("====== Now Round %d ========\n", i)
		}
		addedKeyList = addedKeyList[:0]
		FuzzInit(gobt, cppbt, &addedKeyList, cfg, rs)
		FuzzChange(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzQuery(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzIter(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzDelete(gobt, cppbt, cfg, rs)
	}
}

func FuzzInit(gobt *Tree, cppbt *cb.Tree, addedKeyList *[]uint64, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.MaxInitCount; i++ {
		//if gobt.Len() != cppbt.Len() {
		//	fmt.Printf("Different length go %d cpp %d\n", gobt.Len(), cppbt.Len())
		//}
		assert(gobt.Len() == cppbt.Len(), "Should be same length")
		if gobt.Len() > cfg.MaxLen {
			break
		}
		key := rs.GetUint64()
		value := rs.GetInt64() & Mask48
		//fmt.Printf("Init key: %#v value: %d Len: %d\n", key, value, gobt.Len())
		if rs.GetBool() {
			gobt.Set(key, value)
			cppbt.Set(key, value)
		} else {
			oldVG, oldExistG := gobt.PutNewAndGetOld(key, value)
			oldVC, oldExistC := cppbt.PutNewAndGetOld(key, value)
			//if oldVG!=oldVC {
			//	fmt.Printf(" oldVG %d oldVC %d oldExistG %v oldExistC %v\n", oldVG, oldVC, oldExistG, oldExistC)
			//}
			assert(oldVG==oldVC, "OldValue should be equal")
			assert(oldExistG==oldExistC, "OldExist should be equal")
		}
		*addedKeyList = append(*addedKeyList, key)
	}
}

func FuzzChange(gobt *Tree, cppbt *cb.Tree, addedKeyList []uint64, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.ChangeCount; i++ {
		randIdx := int(uint(rs.GetUint64())%uint(len(addedKeyList)))
		key := addedKeyList[randIdx]
		value := rs.GetInt64() & Mask48
		//fmt.Printf("Change key: %#v value: %d\n", key, value)
		oldVG, oldExistG := gobt.PutNewAndGetOld(key, value)
		oldVC, oldExistC := cppbt.PutNewAndGetOld(key, value)
		assert(oldExistG==oldExistC, "OldExist should be equal")
		if oldExistC {
			//if oldVG != oldVC {
			//	fmt.Printf("oldVG: %d  oldVC: %d key: %#v\n", oldVG, oldVC, key)
			//}
			assert(oldVG==oldVC, "OldValue should be equal")
		}
	}
}

func FuzzQuery(gobt *Tree, cppbt *cb.Tree, addedKeyList []uint64, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.QueryCount; i++ {
		var key uint64
		if rs.GetBool() {
			key = rs.GetUint64()
		} else {
			randIdx := int(uint(rs.GetUint64())%uint(len(addedKeyList)))
			key = addedKeyList[randIdx]
		}
		valueG, okG := gobt.Get(key)
		valueC, okC := cppbt.Get(key)
		assert(okG==okC, "OK should be equal")
		assert(valueG==valueC, "value should be equal")
	}
}

func FuzzIter(gobt *Tree, cppbt *cb.Tree, addedKeyList []uint64, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.IterCount; i++ {
		var key uint64
		if rs.GetBool() {
			key = rs.GetUint64()
		} else {
			randIdx := int(uint(rs.GetUint64())%uint(len(addedKeyList)))
			key = addedKeyList[randIdx]
		}
		iterG, okG := gobt.Seek(key)
		iterC, okC := cppbt.Seek(key)
		assert(okG==okC, "OK should be equal")
		isNext := rs.GetBool()
		var kC, kG uint64
		var vC, vG int64
		var errC, errG error
		//fmt.Printf("Begin Iteration\n")
		//if key == 5460358719145416 {
		//	cppbt.SetDebug(true)
		//}
		for j := 0; j < cfg.IterDistance; j++ {
			//if key == 5460358719145416 {
			//	fmt.Printf("#%d isNext:%v\n", j, isNext)
			//}
			if isNext {
				kC, vC, errC = iterC.Next()
				kG, vG, errG = iterG.Next()
			} else {
				kC, vC, errC = iterC.Prev()
				kG, vG, errG = iterG.Prev()
			}
			//if errC != errG || key == 5460358719145416 {
			//	fmt.Printf("#%d key 0x%x idx %d errC:%#v errG:%#v kG:%v vG:%d isNext:%v\n",
			//	j, key, key>>48, errC, errG, kG, vG, isNext)
			//}
			//if  errC != errG {
			//	for x := 0; x < 10; x++ {
			//		kG, vG, errG = iterG.Prev()
			//		fmt.Printf("errG:%#v kG:%v vG:%d\n", errG, kG, vG)
			//		valueC, okC := cppbt.Get(kG)
			//		fmt.Printf("   valueC %d okC %v\n", valueC, okC)
			//	}
			//	panic("HERE")
			//}
			assert(errC == errG, "err should be equal")
			if errG == io.EOF {
				break
			}
			//fmt.Printf("key:%#v isNext:%v ok:%v\n kC:%#v\n kG:%#v\n",
			//	key, isNext, okG, kC, kG)
			assert(kC == kG, "Key should be equal")
			assert(vC == vG, "Value should be equal")
		}
	}
}

func FuzzDelete(gobt *Tree, cppbt *cb.Tree, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.MaxDelCount; i++ {
		key := rs.GetUint64()
		iterG, okG := gobt.Seek(key)
		iterC, okC := cppbt.Seek(key)
		assert(okG==okC, "OK should be equal")
		kC, _, errC := iterC.Next()
		kG, _, errG := iterG.Next()
		//fmt.Printf("DEL key 0x%x kC 0x%x kG 0x%x errC %d errG %d\n", key, kC, kG, errC, errG)
		assert(errC == errG, "err should be equal")
		if errG == io.EOF {
			continue
		}
		assert(kC == kG, "Keys should be equal")
		gobt.Delete(kG)
		cppbt.Delete(kC)
		assert(gobt.Len() == cppbt.Len(), "Should be same length")
		if gobt.Len() < cfg.MinLen {
			break
		}
	}
}


