package datatree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ProofNode struct {
	SelfHash   [32]byte
	PeerHash   [32]byte
	PeerAtLeft bool
}

type ProofPath struct {
	LeftOfTwig  [11]ProofNode
	RightOfTwig [3]ProofNode
	UpperPath   []ProofNode
	SerialNum   int64
	Root        [32]byte
}

const OtherNodeCount = 1+11+1+3+1
func (pp *ProofPath) ToBytes() []byte {
	res := make([]byte, 0, 8+(len(pp.UpperPath)+OtherNodeCount)*32)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(pp.SerialNum))
	res = append(res, buf[:]...)
	res = append(res, pp.LeftOfTwig[0].SelfHash[:]...)
	for i := 0; i < len(pp.LeftOfTwig); i++ {
		res = append(res, pp.LeftOfTwig[i].PeerHash[:]...)
	}
	res = append(res, pp.RightOfTwig[0].SelfHash[:]...)
	for i := 0; i < len(pp.RightOfTwig); i++ {
		res = append(res, pp.RightOfTwig[i].PeerHash[:]...)
	}
	for i := 0; i < len(pp.UpperPath); i++ {
		res = append(res, pp.UpperPath[i].PeerHash[:]...)
	}
	res = append(res, pp.Root[:]...)
	return res
}

func BytesToProofPath(bz []byte) (*ProofPath, error) {
	pp := &ProofPath{}
	n := len(bz) - 8
	upperCount := n/32-OtherNodeCount
	if n % 32 != 0 || upperCount < 0 {
		return nil, fmt.Errorf("Invalid byte slice length: %d", len(bz))
	}
	pp.UpperPath = make([]ProofNode, upperCount)
	pp.SerialNum = int64(binary.LittleEndian.Uint64(bz[:8]))
	bz = bz[8:]
	copy(pp.LeftOfTwig[0].SelfHash[:], bz[:32])
	bz = bz[32:]
	for i := 0; i < len(pp.LeftOfTwig); i++ {
		copy(pp.LeftOfTwig[i].PeerHash[:], bz[:32])
		pp.LeftOfTwig[i].PeerAtLeft = ((pp.SerialNum >> i) & 1) == 1
		bz = bz[32:]
	}
	copy(pp.RightOfTwig[0].SelfHash[:], bz[:32])
	bz = bz[32:]
	for i := 0; i < len(pp.RightOfTwig); i++ {
		copy(pp.RightOfTwig[i].PeerHash[:], bz[:32])
		pp.RightOfTwig[i].PeerAtLeft = ((pp.SerialNum >> (8+i)) & 1) == 1
		bz = bz[32:]
	}
	for i := 0; i < len(pp.UpperPath); i++ {
		copy(pp.UpperPath[i].PeerHash[:], bz[:32])
		pp.UpperPath[i].PeerAtLeft = ((pp.SerialNum >> (FirstLevelAboveTwig-2+i)) & 1) == 1
		bz = bz[32:]
	}
	copy(pp.Root[:], bz[:32])
	return pp, nil
}

func (pp *ProofPath) Check(complete bool) error {
	for i := 0; i < len(pp.LeftOfTwig)-1; i++ {
		var res []byte
		if pp.LeftOfTwig[i].PeerAtLeft {
			res = hash2(byte(i), pp.LeftOfTwig[i].PeerHash[:], pp.LeftOfTwig[i].SelfHash[:])
		} else {
			res = hash2(byte(i), pp.LeftOfTwig[i].SelfHash[:], pp.LeftOfTwig[i].PeerHash[:])
		}
		if complete {
			copy(pp.LeftOfTwig[i+1].SelfHash[:], res)
		} else {
			if !bytes.Equal(res, pp.LeftOfTwig[i+1].SelfHash[:]) {
				return fmt.Errorf("Mismatch at left path, level: %d", i)
			}
		}
	}
	var leafMTRoot []byte
	if pp.LeftOfTwig[10].PeerAtLeft {
		leafMTRoot = hash2(10, pp.LeftOfTwig[10].PeerHash[:], pp.LeftOfTwig[10].SelfHash[:])
	} else {
		leafMTRoot = hash2(10, pp.LeftOfTwig[10].SelfHash[:], pp.LeftOfTwig[10].PeerHash[:])
	}

	for i := 0; i < 2; i++ {
		var res []byte
		if pp.RightOfTwig[i].PeerAtLeft {
			res = hash2(byte(i+8), pp.RightOfTwig[i].PeerHash[:], pp.RightOfTwig[i].SelfHash[:])
		} else {
			res = hash2(byte(i+8), pp.RightOfTwig[i].SelfHash[:], pp.RightOfTwig[i].PeerHash[:])
		}
		if complete {
			copy(pp.RightOfTwig[i+1].SelfHash[:], res)
		} else {
			if !bytes.Equal(res, pp.RightOfTwig[i+1].SelfHash[:]) {
				fmt.Printf("here %#v\n", pp.RightOfTwig[i])
				return fmt.Errorf("Mismatch at right path, level: %d", i)
			}
		}
	}
	var activeBitsMTL3 []byte
	if pp.RightOfTwig[2].PeerAtLeft {
		activeBitsMTL3 = hash2(10, pp.RightOfTwig[2].PeerHash[:], pp.RightOfTwig[2].SelfHash[:])
	} else {
		activeBitsMTL3 = hash2(10, pp.RightOfTwig[2].SelfHash[:], pp.RightOfTwig[2].PeerHash[:])
	}

	twigRoot := hash2(11, leafMTRoot, activeBitsMTL3)
	if complete {
		copy(pp.UpperPath[0].SelfHash[:], twigRoot)
	} else {
		if !bytes.Equal(twigRoot, pp.UpperPath[0].SelfHash[:]) {
			return fmt.Errorf("Mismatch at twig top")
		}
	}

	for i := 0; i < len(pp.UpperPath); i++ {
		level := FirstLevelAboveTwig - 1 + i
		var res []byte
		if pp.UpperPath[i].PeerAtLeft {
			res = hash2(byte(level), pp.UpperPath[i].PeerHash[:], pp.UpperPath[i].SelfHash[:])
		} else {
			res = hash2(byte(level), pp.UpperPath[i].SelfHash[:], pp.UpperPath[i].PeerHash[:])
		}
		if i < len(pp.UpperPath)-1 {
			if complete {
				copy(pp.UpperPath[i+1].SelfHash[:], res)
			} else {
				if !bytes.Equal(res, pp.UpperPath[i+1].SelfHash[:]) {
					return fmt.Errorf("Mismatch at upper path, level: %d", level)
				}
			}
		} else {
			if !bytes.Equal(res, pp.Root[:]) {
				return fmt.Errorf("Mismatch at root")
			}
		}
	}
	return nil
}

// ===================================================================

func (tree *Tree) GetProof(sn int64) *ProofPath {
	twigID := sn >> TwigShift
	path := &ProofPath{}
	path.SerialNum = sn
	if twigID > tree.youngestTwigID || twigID < 0 {
		panic(fmt.Sprintf("Invalid sn: %d", sn))
	}
	path.UpperPath, path.Root = tree.getUpperPathAndRoot(twigID)
	if path.UpperPath == nil {
		return nil
	}
	if twigID == tree.youngestTwigID {
		path.LeftOfTwig = getLeftPathInMem(tree.mtree4YoungestTwig, sn)
	} else {
		path.LeftOfTwig = getLeftPathOnDisk(tree.twigMtFile, twigID, sn)
	}
	twig, ok := tree.activeTwigs[twigID]
	if ok {
		path.RightOfTwig = getRightPath(twig, sn)
	} else {
		path.RightOfTwig = getRightPath(&NullTwig, sn)
	}
	return path
}

func (tree *Tree) getUpperPathAndRoot(twigID int64) (upperPath []ProofNode, root [32]byte) {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	peerHash, ok := tree.getTwigRoot(twigID^1)
	if !ok {
		peerHash = NullTwig.twigRoot
	}
	selfHash, ok := tree.getTwigRoot(twigID)
	if !ok {
		return
	}
	upperPath = make([]ProofNode, 0, maxLevel-FirstLevelAboveTwig+1)
	upperPath = append(upperPath, ProofNode {
		SelfHash:   selfHash,
		PeerHash:   peerHash,
		PeerAtLeft: (twigID & 1) != 0,
	})
	for level, n := FirstLevelAboveTwig, twigID/2; level < maxLevel; level, n = level+1, n/2 {
		//fmt.Printf("level: %d  n: %d maxLevel: %d ok:%v\n", level, n, maxLevel, ok)
		upperPath = append(upperPath, ProofNode {
			SelfHash:   *tree.nodes[Pos(level, n)],
			PeerHash:   *tree.nodes[Pos(level, n^1)],
			PeerAtLeft: (n & 1) != 0,
		})
	}
	return upperPath, *tree.nodes[Pos(maxLevel, 0)]
}

func getRightPath(twig *Twig, sn int64) (right [3]ProofNode) {
	n := (sn & TwigMask)
	self := n / 256
	peer := self ^ 1
	copy(right[0].SelfHash[:], twig.activeBits[self*32 : self*32+32])
	copy(right[0].PeerHash[:], twig.activeBits[peer*32 : peer*32+32])
	right[0].PeerAtLeft = (peer & 1 ) == 0

	self = n / 512
	peer = self ^ 1
	right[1].SelfHash = twig.activeBitsMTL1[self]
	right[1].PeerHash = twig.activeBitsMTL1[peer]
	right[1].PeerAtLeft = (peer & 1 ) == 0

	self = n / 1024
	peer = self ^ 1
	right[2].SelfHash = twig.activeBitsMTL2[self]
	right[2].PeerHash = twig.activeBitsMTL2[peer]
	right[2].PeerAtLeft = (peer & 1 ) == 0
	return
}

func getLeftPath(sn int64, getHash func(int) [32]byte) (left [11]ProofNode) {
	n := sn & TwigMask
	for stripe, level := 2048, 0; level <= 10; stripe, level = stripe/2, level+1 {
		self := n >> level
		peer := self ^ 1
		left[level] = ProofNode{
			SelfHash:   getHash(stripe+int(self)),
			PeerHash:   getHash(stripe+int(peer)),
			PeerAtLeft: (peer & 1 ) == 0,
		}
	}
	return
}

func getLeftPathInMem(mt4twig [4096][32]byte, sn int64) (left [11]ProofNode) {
	return getLeftPath(sn, func(i int) [32]byte {
		return mt4twig[i]
	})
}

func getLeftPathOnDisk(tf *TwigMtFile, twigID int64, sn int64) (left [11]ProofNode) {
	return getLeftPath(sn, func(i int) (res [32]byte) {
		copy(res[:], tf.GetHashNode(twigID, i))
		return
	})
}

