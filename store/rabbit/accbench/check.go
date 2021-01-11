package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/coinexchain/randsrc"
	sha256 "github.com/minio/sha256-simd"
	"github.com/pkg/profile"

	moeingads "github.com/moeing-chain/MoeingADS"
	"github.com/moeing-chain/MoeingADS/datatree"
	"github.com/moeing-chain/MoeingADS/store"
	"github.com/moeing-chain/MoeingADS/store/rabbit"
)

const (
	WithRabbit = true
)

func CheckAccountsInBlock(snList []uint32, root *store.RootStore) {
	trunk := root.GetTrunkStore().(*store.TrunkStore)
	sharedIdx := int64(-1)
	datatree.ParrallelRun(NumWorkersInBlock, func(workerID int) {
		rbt := rabbit.NewRabbitStore(trunk)
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(snList)) {
				break
			}
			if WithRabbit {
				CheckAccountWithRabbit(snList[myIdx], rbt)
			} else {
				CheckAccountWithRoot(snList[myIdx], root)
			}
		}
	})
}

func CompareAccounts(acc, accRD *Account) {
	if !bytes.Equal(acc.Address(), accRD.Address()) {
		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf("======== acc with zero coin ========\n"))
		buffer.WriteString(acc.GetInfo())
		buffer.WriteString(fmt.Sprintf("======== read acc ========\n"))
		buffer.WriteString(accRD.GetInfo())
		fmt.Printf(buffer.String())
		panic("Different Address!")
	}
	if acc.GetCoinCount() != accRD.GetCoinCount() {
		panic("Different Coin Count!")
	}
	for i := 0; i < acc.GetCoinCount(); i++ {
		coinA := acc.GetTokenID(i)
		coinB := accRD.GetTokenID(i)
		if !bytes.Equal(coinA[:], coinB[:]) {
			panic(fmt.Sprintf("Different Coin Type at %d", i))
		}
	}
}

func CheckAccountWithRoot(sn uint32, root *store.RootStore) {
	addr := SNToAddr(int64(sn))
	hash := sha256.Sum256(addr[:])
	var sk [rabbit.KeySize]byte
	copy(sk[:], hash[:])
	//sk[0] |= 0x1
	bz := root.Get(sk[:])
	cachedValue := rabbit.BytesToCachedValue(bz)
	if cachedValue.IsEmpty() {
		panic("Read Empty Entry")
	}
	bz = cachedValue.GetValue()
	var accRD Account
	accRD.FromBytes(bz)

	acc := GenerateZeroCoinAccount(int64(sn))
	CompareAccounts(&acc, &accRD)
}

func CheckAccountWithRabbit(sn uint32, rbt rabbit.RabbitStore) {
	acc := GenerateZeroCoinAccount(int64(sn))
	var accRD Account
	bz := rbt.Get(acc.Address())
	if len(bz) == 0 {
		panic("Cannot find account")
	}
	accRD.FromBytes(bz)
	CompareAccounts(&acc, &accRD)
}

func ShuffleSNList(snList []uint32, rs randsrc.RandSrc) {
	for i := 0; i < len(snList); i++ {
		a := int(rs.GetUint32()) % len(snList)
		b := int(rs.GetUint32()) % len(snList)
		snList[a], snList[b] = snList[b], snList[a]
	}
}

func GenerateRandomSNList(numAccounts int, rs randsrc.RandSrc) []uint32 {
	snList := make([]uint32, numAccounts)
	for i := range snList {
		snList[i] = uint32(i)
	}
	ShuffleSNList(snList, rs)
	return snList
}

func DumpRandomSNList(snList []uint32) {
	out, err := os.Create("randomlist.dat")
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outWr := bufio.NewWriterSize(out, 1024*1024*16)
	for _, sn := range snList {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], sn)
		_, err := outWr.Write(buf[:])
		if err != nil {
			panic(err)
		}
	}
	err = outWr.Flush()
}

func ReadOneBlockOfAccounts(f *os.File, n int) (res [NumNewAccountsInBlock]uint32) {
	f.Seek(int64(n*NumNewAccountsInBlock), os.SEEK_SET)
	fin := bufio.NewReaderSize(f, 1024*1024*16)
	for i := range res {
		var buf [4]byte
		fin.Read(buf[:])
		res[i] = binary.LittleEndian.Uint32(buf[:])
	}
	return
}

func RunCheckAccounts(numAccounts int, randFilename string) {
	fmt.Printf("Start %f\n", float64(time.Now().UnixNano())/1000000000.0)
	rs := randsrc.NewRandSrcFromFile(randFilename)

	DumpRandomSNList(GenerateRandomSNList(numAccounts, rs))
	f, err := os.Open("randomlist.dat")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	runtime.GC()

	mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	root := store.NewRootStore(mads, nil)

	fmt.Printf("After Load %f\n", float64(time.Now().UnixNano())/1000000000.0)
	if numAccounts%NumNewAccountsInBlock != 0 {
		panic("numAccounts % NumNewAccountsInBlock != 0")
	}
	numBlocks := numAccounts / NumNewAccountsInBlock
	pjob := profile.Start()
	for i := 0; i < numBlocks; i++ {
		root.SetHeight(int64(i))
		if i%100 == 0 {
			fmt.Printf("Now %d of %d, %d\n", i, numBlocks, root.ActiveCount())
		}
		snList := ReadOneBlockOfAccounts(f, i)
		CheckAccountsInBlock(snList[:], root)
	}
	pjob.Stop()
	fmt.Printf("Read Finished %f\n", float64(time.Now().UnixNano())/1000000000.0)

	root.Close()
}
