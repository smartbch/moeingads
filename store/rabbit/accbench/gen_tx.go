package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io/ioutil"
	"os"
	"time"

	"github.com/coinexchain/randsrc"
	sha256 "github.com/minio/sha256-simd"
	"github.com/mmcloughlin/meow"

	"github.com/moeing-chain/MoeingADS/store/rabbit"
)

const (
	TxLen       = 32
	MaxTryCount = 20
)

type Tx struct {
	FromNum [rabbit.KeySize]byte
	ToNum   [rabbit.KeySize]byte
	Amount  [rabbit.KeySize]byte
	CoinID  [ShortIDLen]byte
}

func (tx Tx) UpdateHash64(h hash.Hash64) {
	h.Write(tx.FromNum[:])
	h.Write(tx.ToNum[:])
	h.Write(tx.Amount[:])
	h.Write(tx.CoinID[:])

}

func (tx Tx) ToBytes() []byte {
	res := make([]byte, 0, TxLen)
	res = append(res, tx.FromNum[:]...)
	res = append(res, tx.ToNum[:]...)
	res = append(res, tx.Amount[:]...)
	res = append(res, tx.CoinID[:]...)
	return res
}

func ParseTx(bz [TxLen]byte) (tx Tx) {
	copy(tx.FromNum[:], bz[0:8])
	copy(tx.ToNum[:], bz[8:16])
	copy(tx.Amount[:], bz[16:24])
	copy(tx.CoinID[:], bz[24:32])
	return
}

// Given an account's address, calculate its "card number"
func getNum(addr2num map[[AddrLen]byte]uint64, addr [AddrLen]byte) (res [rabbit.KeySize]byte) {
	if num, ok := addr2num[addr]; ok {
		// if the card number was recorded, use it
		binary.LittleEndian.PutUint64(res[:], num)
		return
	}
	// otherwise we re-compute the card number, with one hop
	hash := sha256.Sum256(addr[:])
	copy(res[:], hash[:])
	res[0] |= 0x1
	return
}

// generate a random Tx, whose from-account and to-account have at least one coin in common
func GenerateTx(totalAccounts uint64, addr2num map[[AddrLen]byte]uint64, rs randsrc.RandSrc) *Tx {
	fromSN := int64(rs.GetUint64() % totalAccounts)
	fromCoinList := GetCoinList(fromSN)
	if len(fromCoinList) == 0 {
		panic("zero length")
	}
	fromCoinMap := make(map[uint8]struct{}, len(fromCoinList))
	for _, coinType := range fromCoinList {
		fromCoinMap[coinType] = struct{}{}
	}
	tx := Tx{FromNum: getNum(addr2num, SNToAddr(fromSN))}
	binary.LittleEndian.PutUint64(tx.Amount[:], rs.GetUint64())
	toSN := int64(rs.GetUint64() % totalAccounts)
	for i := 0; i < MaxTryCount; i++ {
		toCoinList := GetCoinList(toSN)
		coinTypeOfBoth := -1
		for _, coinType := range toCoinList {
			if _, ok := fromCoinMap[coinType]; ok {
				coinTypeOfBoth = int(coinType)
				break
			}
		}
		if coinTypeOfBoth > 0 { // both of them have one same coin
			tx.ToNum = getNum(addr2num, SNToAddr(int64(toSN)))
			tx.CoinID = CoinIDList[coinTypeOfBoth]
			return &tx
		} else { //try another to-account
			toSN = int64(rs.GetUint64() % totalAccounts)
		}
	}
	return nil
}

func RunGenerateTxFile(epochCount int, totalAccounts uint64, jsonFilename, randFilename, outFilename string) {
	fmt.Printf("Start %f\n", float64(time.Now().UnixNano())/1e9)
	//mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
	//if err != nil {
	//	panic(err)
	//}
	//root := store.NewRootStore(mads, nil, nil)

	jsonFile, err := os.Open(jsonFilename)
	if err != nil {
		panic(err)
	}
	bz, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		panic(err)
	}
	addr2num := make(map[[AddrLen]byte]uint64)
	if len(bz) != 0 {
		err = json.Unmarshal(bz, &addr2num)
		if err != nil {
			panic(err)
		}
	}

	rs := randsrc.NewRandSrcFromFile(randFilename)
	out, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	// one account can just occur once in an epoch, otherwise there will be conflicts
	// we must filter away the TXs which conflict with existing TXs in the epoch
	for i := 0; i < epochCount; i++ {
		if i%100 == 0 {
			fmt.Printf("Now %d of %d\n", i, epochCount)
		}
		txCount := 0
		touchedNum := make(map[[rabbit.KeySize]byte]struct{}, 2*NumTxInEpoch)
		hash64 := meow.New64(0)
		for {
			tx := GenerateTx(totalAccounts, addr2num, rs)
			for tx == nil { //re-try
				tx = GenerateTx(totalAccounts, addr2num, rs)
			}

			//bz := root.Get(tx.FromNum[:])
			//if len(bz) == 0 {
			//	panic(fmt.Sprintf("tx.FromNum[:] not found %#v", tx.FromNum[:]))
			//}
			//bz = root.Get(tx.ToNum[:])
			//if len(bz) == 0 {
			//	panic(fmt.Sprintf("tx.FromNum[:] not found %#v", tx.ToNum[:]))
			//}

			_, fromConflict := touchedNum[tx.FromNum]
			_, toConflict := touchedNum[tx.ToNum]
			if fromConflict || toConflict {
				//fmt.Printf("Found Conflict\n")
				continue
			}
			touchedNum[tx.FromNum] = struct{}{}
			touchedNum[tx.ToNum] = struct{}{}
			_, err := out.Write(tx.ToBytes())
			tx.UpdateHash64(hash64)
			if err != nil {
				panic(err)
			}
			txCount++
			if txCount == NumTxInEpoch {
				break
			}
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], hash64.Sum64())
		_, err := out.Write(buf[:])
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("Finished %f\n", float64(time.Now().UnixNano())/1e9)
}
