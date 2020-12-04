package datatree

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkEqual(pp, other *ProofPath) string {
	if len(pp.UpperPath) != len(other.UpperPath) {
		return "UpperPath's length not equal"
	}
	if pp.SerialNum != other.SerialNum {
		return "SerialNum not equal"
	}
	if !bytes.Equal(pp.LeftOfTwig[0].SelfHash[:], other.LeftOfTwig[0].SelfHash[:]) {
		return "LeftOfTwig.SelfHash not equal"
	}
	for i := 0; i < 11; i++ {
		if !bytes.Equal(pp.LeftOfTwig[i].PeerHash[:], other.LeftOfTwig[i].PeerHash[:]) {
			return "LeftOfTwig.PeerHash not equal"
		}
		if pp.LeftOfTwig[i].PeerAtLeft != other.LeftOfTwig[i].PeerAtLeft {
			return fmt.Sprintf("LeftOfTwig.PeerAtLeft[%d] not equal %v %d", i, other.LeftOfTwig[i].PeerAtLeft, other.SerialNum)
		}
	}
	if !bytes.Equal(pp.RightOfTwig[0].SelfHash[:], other.RightOfTwig[0].SelfHash[:]) {
		return "RightOfTwig.SelfHash not equal"
	}
	for i := 0; i < 3; i++ {
		if !bytes.Equal(pp.RightOfTwig[i].PeerHash[:], other.RightOfTwig[i].PeerHash[:]) {
			return "RightOfTwig.PeerHash not equal"
		}
		if pp.RightOfTwig[i].PeerAtLeft != other.RightOfTwig[i].PeerAtLeft {
			return "RightOfTwig.PeerAtLeft not equal"
		}
	}
	for i := 0; i < len(pp.UpperPath); i++ {
		if !bytes.Equal(pp.UpperPath[i].PeerHash[:], other.UpperPath[i].PeerHash[:]) {
			return "UpperPath.PeerHash not equal"
		}
		if pp.UpperPath[i].PeerAtLeft != other.UpperPath[i].PeerAtLeft {
			return "UpperPath.PeerAtLeft not equal"
		}
	}
	if !bytes.Equal(pp.Root[:], other.Root[:]) {
		return "Root not equal"
	}
	return ""
}

func TestTreeProof(t *testing.T) {
	dirName := "./DataTree"
	os.RemoveAll(dirName)
	os.Mkdir(dirName, 0700)
	deactSNList := make([]int64, 0, 2048+20)
	for i := 0; i < 2048; i++ {
		deactSNList = append(deactSNList, int64(i))
	}
	deactSNList = append(deactSNList, []int64{5000, 5500, 5700, 5813, 6001}...)
	tree, _, _ := buildTestTree(dirName, deactSNList, TwigMask*4, 1600)
	fmt.Printf("build finished\n")
	tree.EvictTwig(0)
	tree.EndBlock()
	CheckHashConsistency(tree)

	//require.Nil(t, tree.GetProof(0))
	//require.Nil(t, tree.GetProof(2049))

	maxSN := TwigMask*4+1600
	for i := 0; i < maxSN; i++ {
		//fmt.Printf("---------- %d ----------\n", i)
		proofPath := tree.GetProof(int64(i))
		if proofPath == nil {
			panic("Proof not found")
		}
		err := proofPath.Check(false)
		require.Nil(t, err)

		bz := proofPath.ToBytes()
		path2, err := BytesToProofPath(bz)
		require.Nil(t, err)
		require.Equal(t, "", checkEqual(proofPath, path2))
		err = path2.Check(true)
		require.Nil(t, err)
	}

	tree.Close()
	os.RemoveAll(dirName)
}
