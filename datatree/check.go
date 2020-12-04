package datatree

import (
	"bytes"
	"fmt"

	sha256 "github.com/minio/sha256-simd"
)

func checkMT(mt [4096][32]byte) {
	for stripe, level := 1, byte(10); stripe <= 1024; stripe, level = stripe*2, level-1 {
		for i := stripe; i < 2*stripe; i++ {
			b := append(append([]byte{level}, mt[2*i][:]...), mt[2*i+1][:]...)
			//fmt.Printf("Check %d-%d(%d) %d(%d) %d(%d)\n", level, i-stripe, i,
			//	2*i-stripe*2, 2*i, 2*i+1-stripe*2, 2*i+1)
			sum := sha256.Sum256(b)
			if !bytes.Equal(mt[i][:], sum[:]) {
				panic(fmt.Sprintf("Mismatch %d-%d %d %d", level, i, 2*i, 2*i+1))
			}
		}
	}
}

func checkUpperNodes(tree *Tree) {
	for pos, parentHash := range tree.nodes {
		level := int64(pos)>>56
		n := (int64(pos)<<8)>>8
		//fmt.Printf("Checking %d-%d %d- %d %d\n", level, n, level-1, 2*n, 2*n+1)
		var leftChild, rightChild [32]byte
		if level == int64(FirstLevelAboveTwig) {
			var ok bool
			leftChild, ok = tree.getTwigRoot(int64(2*n))
			if !ok {
				continue
			}
			rightChild, ok = tree.getTwigRoot(int64(2*n+1))
			if !ok {
				rightChild = NullTwig.twigRoot
			}
			//if Debug {
			//	fmt.Printf("rightTwig:%v ok:%v\n", rightChild, ok)
			//	for i := range tree.activeTwigs {
			//		fmt.Printf("active %d\n", i)
			//	}
			//}
		} else {
			leftChildPtr, ok := tree.nodes[Pos(int(level-1), 2*n)]
			if !ok {
				continue
			}
			rightChildPtr, ok := tree.nodes[Pos(int(level-1), 2*n+1)]
			if !ok {
				continue
			}
			leftChild, rightChild = *leftChildPtr, *rightChildPtr
		}
		h := sha256.Sum256(append(append([]byte{byte(level-1)}, leftChild[:]...), rightChild[:]...))
		if !bytes.Equal(h[:], (*parentHash)[:]) {
			fmt.Printf("left: %#v right: %#v\n", leftChild, rightChild)
			panic(fmt.Sprintf("Mismatch at %d-%d l:%d r:%d", level, n, 2*n, 2*n+1))
		}
	}
}

func hashEqual(tag string, a, b [32]byte) {
	if !bytes.Equal(a[:], b[:]) {
		panic(tag+"Not Equal")
	}
}

func checkTwig(twig *Twig) {
	hashEqual("L1-0", twig.activeBitsMTL1[0], sha256.Sum256(append([]byte{8}, twig.activeBits[64*0:64*1]...)))
	hashEqual("L1-1", twig.activeBitsMTL1[1], sha256.Sum256(append([]byte{8}, twig.activeBits[64*1:64*2]...)))
	hashEqual("L1-2", twig.activeBitsMTL1[2], sha256.Sum256(append([]byte{8}, twig.activeBits[64*2:64*3]...)))
	hashEqual("L1-3", twig.activeBitsMTL1[3], sha256.Sum256(append([]byte{8}, twig.activeBits[64*3:64*4]...)))
	hashEqual("L2-0", twig.activeBitsMTL2[0], sha256.Sum256(append([]byte{9},
	       append(twig.activeBitsMTL1[0][:], twig.activeBitsMTL1[1][:]...)...)))
	hashEqual("L2-1", twig.activeBitsMTL2[1], sha256.Sum256(append([]byte{9},
	       append(twig.activeBitsMTL1[2][:], twig.activeBitsMTL1[3][:]...)...)))
	hashEqual("L3", twig.activeBitsMTL3, sha256.Sum256(append([]byte{10},
	       append(twig.activeBitsMTL2[0][:], twig.activeBitsMTL2[1][:]...)...)))
	hashEqual("Top", twig.twigRoot, sha256.Sum256(append([]byte{11},
		append(twig.leftRoot[:], twig.activeBitsMTL3[:]...)...)))
}

func checkAllTwigs(tree *Tree) {
	for _, twig := range tree.activeTwigs {
		checkTwig(twig)
	}
}

func CheckHashConsistency(tree *Tree) {
	checkAllTwigs(tree)
	checkUpperNodes(tree)
	checkMT(tree.mtree4YoungestTwig)
}
