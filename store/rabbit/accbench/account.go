package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"sync/atomic"

	"github.com/coinexchain/randsrc"
	"github.com/dterei/gotsc"
	sha256 "github.com/minio/sha256-simd"

	moeingads "github.com/moeing-chain/MoeingADS"
	"github.com/moeing-chain/MoeingADS/datatree"
	"github.com/moeing-chain/MoeingADS/store"
	"github.com/moeing-chain/MoeingADS/store/rabbit"
	"github.com/moeing-chain/MoeingADS/store/types"
)

var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd   = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

const (
	MaxCoinCount = 20  // maximum count of coin types in an account
	NumCoinType  = 100 // total coin types in the system

	AddrLen                 = 20
	ShortIDLen              = 8
	AmountLen               = 32
	EntryLen                = ShortIDLen + AmountLen
	AddressOffset           = 0
	SequenceOffset          = AddressOffset + AddrLen
	NativeTokenAmountOffset = SequenceOffset + 8
	ERC20TokenOffset        = NativeTokenAmountOffset + AmountLen

	NumNewAccountsInBlock   = 20000
	NumWorkersInBlock       = 64
	NumNewAccountsPerWorker = NumNewAccountsInBlock / NumWorkersInBlock
)

var Phase1Time, Phase2Time, Phase3Time, tscOverhead uint64

// convert coin type as an integer to coin id as a hash
func CoinTypeToCoinID(i int) (res [ShortIDLen]byte) {
	hash := sha256.Sum256([]byte(fmt.Sprintf("coin%d", i)))
	copy(res[:], hash[:])
	return
}

var CoinIDList [NumCoinType][ShortIDLen]byte

func init() { //initialize CoinIDList
	for i := range CoinIDList {
		CoinIDList[i] = CoinTypeToCoinID(i)
	}
}

type Coin struct {
	ID     [ShortIDLen]byte
	Amount [AmountLen]byte
}

type Account struct {
	bz        []byte //all the operations directly read/write raw bytes
	coinCount int
}

var _ types.Serializable = &Account{}

func (acc *Account) ToBytes() []byte {
	return acc.bz
}

func (acc *Account) FromBytes(bz []byte) {
	if len(bz) < ERC20TokenOffset || len(bz[ERC20TokenOffset:])%EntryLen != 0 {
		panic(fmt.Sprintf("Invalid bytes for Account: %#v", bz))
	}
	acc.bz = bz
	acc.coinCount = len(bz[ERC20TokenOffset:]) / EntryLen
}

func (acc *Account) DeepCopy() interface{} {
	return &Account{
		bz:        append([]byte{}, acc.bz...),
		coinCount: acc.coinCount,
	}
}

func NewAccount(addr [AddrLen]byte, sequence int64, nativeTokenAmount [AmountLen]byte, coins []Coin) Account {
	bz := make([]byte, AddrLen+8+AmountLen+len(coins)*EntryLen)
	copy(bz[AddressOffset:], addr[:])
	binary.LittleEndian.PutUint64(bz[SequenceOffset:SequenceOffset+8], uint64(sequence))
	copy(bz[NativeTokenAmountOffset:], nativeTokenAmount[:])
	start := ERC20TokenOffset
	if len(coins) > MaxCoinCount {
		panic("Too many coins")
	}
	for _, coin := range coins {
		copy(bz[start:], coin.ID[:])
		copy(bz[start+ShortIDLen:], coin.Amount[:])
		start += EntryLen
	}
	return Account{bz: bz, coinCount: len(coins)}
}

func (acc *Account) GetCoinCount() int {
	return acc.coinCount
}

func (acc Account) GetInfo() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Account %#v seq %x %d\n", acc.Address(), acc.GetSequence(), acc.GetSequence()>>32))
	for i := 0; i < acc.coinCount; i++ {
		buffer.WriteString(fmt.Sprintf("   Coin#%d %#v\n", i, acc.GetTokenID(i)))
	}
	return buffer.String()
}

func (acc Account) Address() []byte {
	return acc.bz[AddressOffset : AddressOffset+AddrLen]
}

func (acc Account) GetSequence() int64 {
	return int64(binary.LittleEndian.Uint64(acc.bz[SequenceOffset : SequenceOffset+8]))
}

func (acc Account) SetSequence(seq int64) {
	binary.LittleEndian.PutUint64(acc.bz[SequenceOffset:SequenceOffset+8], uint64(seq))
}

func (acc Account) GetNativeAmount() []byte {
	return acc.bz[NativeTokenAmountOffset : NativeTokenAmountOffset+AmountLen]
}

func (acc Account) SetNativeAmount(amount [AmountLen]byte) {
	copy(acc.bz[NativeTokenAmountOffset:], amount[:])
}

func (acc Account) GetTokenID(i int) (res [ShortIDLen]byte) {
	start := ERC20TokenOffset + i*EntryLen
	copy(res[:], acc.bz[start:start+ShortIDLen])
	return
}

func (acc Account) GetTokenAmount(i int) []byte {
	start := ERC20TokenOffset + i*EntryLen + ShortIDLen
	return acc.bz[start : start+AmountLen]
}

func (acc Account) SetTokenAmount(i int, amount [AmountLen]byte) {
	start := ERC20TokenOffset + i*EntryLen + ShortIDLen
	copy(acc.bz[start:], amount[:])
}

func (acc Account) Find(tokenID [ShortIDLen]byte) int {
	i := sort.Search(acc.coinCount, func(i int) bool {
		id := acc.GetTokenID(i)
		return bytes.Compare(id[:], tokenID[:]) >= 0
	})
	id := acc.GetTokenID(i)
	if i < acc.coinCount && bytes.Equal(id[:], tokenID[:]) { //present
		return i
	} else { // not present
		return -1
	}
}

// the serial number of an account decides its address
func SNToAddr(accountSN int64) (addr [AddrLen]byte) {
	hash := sha256.Sum256([]byte(fmt.Sprintf("address%d", accountSN)))
	copy(addr[:], hash[:])
	return
}

// the serial number of an account decides which types of coins it has
func GetCoinList(accountSN int64) []uint8 {
	hash := sha256.Sum256([]byte(fmt.Sprintf("listofcoins%d", accountSN)))
	coinCount := 1 + hash[0]%MaxCoinCount
	res := make([]uint8, coinCount)
	for i := range res {
		res[i] = hash[i+1] % NumCoinType
	}
	return res
}

func BigIntToBytes(i *big.Int) (res [AmountLen]byte) {
	bz := i.Bytes()
	if len(bz) > AmountLen {
		panic("Too large")
	}
	startingZeros := len(res) - len(bz)
	copy(res[startingZeros:], bz)
	return res
}

func GetRandAmount(rs randsrc.RandSrc) [AmountLen]byte {
	i := big.NewInt(int64(rs.GetUint32()))
	i.Lsh(i, 128)
	return BigIntToBytes(i)
}

func GenerateZeroCoinAccount(accountSN int64) Account {
	var zero [AmountLen]byte
	coinList := GetCoinList(accountSN)
	coins := make([]Coin, len(coinList))
	for i := range coins {
		coinType := int(coinList[i])
		coins[i].ID = CoinIDList[coinType]
	}
	sort.Slice(coins, func(i, j int) bool {
		return bytes.Compare(coins[i].ID[:], coins[j].ID[:]) < 0
	})
	return NewAccount(SNToAddr(accountSN), accountSN<<32, zero, coins)
}

func GenerateAccount(accountSN int64, rs randsrc.RandSrc) Account {
	nativeTokenAmount := GetRandAmount(rs)
	coinList := GetCoinList(accountSN)
	coins := make([]Coin, len(coinList))
	for i := range coins {
		coinType := int(coinList[i])
		coins[i].ID = CoinIDList[coinType]
		coins[i].Amount = GetRandAmount(rs)
	}
	sort.Slice(coins, func(i, j int) bool {
		return bytes.Compare(coins[i].ID[:], coins[j].ID[:]) < 0
	})
	return NewAccount(SNToAddr(accountSN), accountSN<<32, nativeTokenAmount, coins)
}

func RunGenerateAccounts(numAccounts int, randFilename string, jsonFile string) {
	tscOverhead = gotsc.TSCOverhead()

	addr2num := make(map[[AddrLen]byte]uint64)
	rs := randsrc.NewRandSrcFromFile(randFilename)
	mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	root := store.NewRootStore(mads, nil)

	if numAccounts%NumNewAccountsInBlock != 0 {
		panic("numAccounts % NumNewAccountsInBlock != 0")
	}
	numBlocks := numAccounts / NumNewAccountsInBlock
	start := gotsc.BenchStart()
	for i := 0; i < numBlocks; i++ {
		root.SetHeight(int64(i))
		if i%10 == 0 {
			fmt.Printf("Now %d of %d, %d\n", i, numBlocks, root.ActiveCount())
		}
		trunk := root.GetTrunkStore().(*store.TrunkStore)
		GenerateAccountsInBlock(int64(i*NumNewAccountsInBlock), trunk, rs, addr2num)
		start := gotsc.BenchStart()
		trunk.Close(true)
		Phase3Time += gotsc.BenchEnd() - start - tscOverhead
	}

	b, err := json.Marshal(addr2num)
	out, err := os.OpenFile(jsonFile, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		panic(err)
	}
	fmt.Printf("len(addr2num): %d, addr2num: %s\n", len(addr2num), string(b))
	out.Write(b)
	out.Close()

	fmt.Printf("total tsc time %d\n", gotsc.BenchEnd()-start)
	fmt.Printf("phase1 time %d\n", Phase1Time)
	fmt.Printf("phase2 time %d\n", Phase2Time)
	fmt.Printf("phase3 time %d\n", Phase3Time)
	fmt.Printf("phaseTrunk time %d\n", store.PhaseTrunkTime)
	fmt.Printf("phaseEndW time %d\n", store.PhaseEndWriteTime)
	fmt.Printf("moeingads.phase0 time %d\n", moeingads.Phase0Time)
	fmt.Printf("moeingads.pha1n2 time %d\n", moeingads.Phase1n2Time)
	fmt.Printf("moeingads.phase1 time %d\n", moeingads.Phase1Time)
	fmt.Printf("moeingads.phase2 time %d\n", moeingads.Phase2Time)
	fmt.Printf("moeingads.phase3 time %d\n", moeingads.Phase3Time)
	fmt.Printf("moeingads.phase4 time %d\n", moeingads.Phase4Time)
	fmt.Printf("dat.ph1 time %d\n", datatree.Phase1Time)
	fmt.Printf("dat.ph2 time %d\n", datatree.Phase2Time)
	fmt.Printf("dat.ph3 time %d\n", datatree.Phase3Time)
	fmt.Printf("write time %d\n", datatree.TotalWriteTime)
	fmt.Printf("read time %d\n", datatree.TotalReadTime)
	fmt.Printf("sync time %d\n", datatree.TotalSyncTime)
	mads.PrintMetaInfo()
	root.Close()
}

func GenerateAccountsInBlock(startSN int64, trunk *store.TrunkStore, rs randsrc.RandSrc, addr2num map[[AddrLen]byte]uint64) {
	start := gotsc.BenchStart()

	var accounts [NumNewAccountsInBlock]Account
	for i := 0; i < NumNewAccountsInBlock; i++ {
		accounts[i] = GenerateAccount(startSN+int64(i), rs)
	}
	////////
	sharedIdx := int64(-1)
	var rbtList [NumWorkersInBlock]rabbit.RabbitStore
	// Parallel execution
	datatree.ParrallelRun(NumWorkersInBlock, func(workerID int) {
		rbt := rabbit.NewRabbitStore(trunk)
		rbtList[workerID] = rbt
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(accounts)) {
				break
			}
			WriteAccount(accounts[myIdx], rbt, addr2num)
		}
	})
	Phase1Time += gotsc.BenchEnd() - start - tscOverhead
	start = gotsc.BenchStart()
	// Serial collection
	touchedShortKey := make(map[[rabbit.KeySize]byte]struct{}, NumNewAccountsInBlock)
	for i, rbt := range rbtList {
		hasConflict := false
		rbt.ScanAllShortKeys(func(key [rabbit.KeySize]byte, dirty bool) (stop bool) {
			if _, ok := touchedShortKey[key]; ok {
				hasConflict = true
				return true
			}
			return false
		})
		if hasConflict { // re-execute it serially
			panic(fmt.Sprintf("hasConflict %d\n", i))
		}
		rbt.ScanAllShortKeys(func(key [rabbit.KeySize]byte, dirty bool) (stop bool) {
			touchedShortKey[key] = struct{}{}
			return false
		})
		rbt.Close()
		rbt.WriteBack()
	}
	Phase2Time += gotsc.BenchEnd() - start - tscOverhead
}

func WriteAccount(acc Account, rbt rabbit.RabbitStore, addr2num map[[AddrLen]byte]uint64) {
	rbt.Set(acc.Address(), acc.ToBytes())
	path, ok := rbt.GetShortKeyPath(acc.Address())
	if !ok {
		panic("Cannot get the object which was just set")
	}
	if len(path) > 1 { // special cases: more than one hop
		var addr [AddrLen]byte
		copy(addr[:], acc.Address())
		n := binary.LittleEndian.Uint64(path[len(path)-1][:])
		addr2num[addr] = n
	}
}
