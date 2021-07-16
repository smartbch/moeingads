package fuzz

import (
	"testing"
)

// go test -tags debug -c -coverpkg github.com/smartbch/moeingads/... .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=2000 ./fuzz.test -test.coverprofile a.out

func Test1(t *testing.T) {
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
		MaxReadCountInTx:       10,
		MaxCheckProofCountInTx: 2,
		MaxWriteCountInTx:      10,
		MaxDeleteCountInTx:     10,
		MaxTxCountInEpoch:      100, // For rabbit, we cannot avoid inter-tx dependency prehand, but it seldom happens
		MaxEpochCountInBlock:   500,
		EffectiveBits:          0xFF000000_000000FF,
		MaxActiveCount:         32 * 1024,
		MaxValueLength:         256,
		TxSucceedRatio:         0.85,
		BlockSucceedRatio:      0.95,
		BlockPanicRatio:        0.03,
		RootType:               "Real", //"MockDataTree", //"MockRoot",
		ConsistencyEveryNBlock: 200,
		PruneEveryNBlock:       100,
		KeepRecentNBlock:       100,
		ReloadEveryNBlock:      500,
	}
	runTest(cfg2)

}
