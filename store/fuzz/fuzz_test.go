package fuzz

import (
	"testing"
)

// go test -tags cppbtree -c -coverpkg github.com/moeing-chain/MoeingADS/store .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=1000 ./fuzz.test

func Test1(t *testing.T) {
	//cfg1 := &FuzzConfig {
	//	MaxReadCountInTx:     10,
	//	MaxWriteCountInTx:    10,
	//	MaxDeleteCountInTx:   10,
	//	MaxTxCountInEpoch:    100,
	//	MaxEpochCountInBlock: 5,
	//	EffectiveBits:        16,
	//	MaxActiveCount:       -1,
	//	TxSucceedRatio:       0.85,
	//	BlockSucceedRatio:    0.95,
	//	RootType:             "Real",
	//}
	//runTest(cfg1)

	cfg2 := &FuzzConfig{
		MaxReadCountInTx:     10,
		MaxWriteCountInTx:    10,
		MaxDeleteCountInTx:   10,
		MaxTxCountInEpoch:    10, // For rabbit, we cannot avoid inter-tx dependency prehand, but it seldom happens
		MaxEpochCountInBlock: 500,
		EffectiveBits:        16,
		MaxActiveCount:       256 * 256 / 16,
		TxSucceedRatio:       0.85,
		BlockSucceedRatio:    0.95,
		RootType:             "Real", //"MockDataTree", //"MockRoot",
	}
	runTest(cfg2)

}
