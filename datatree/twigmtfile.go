package datatree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mmcloughlin/meow"
)

type TwigMtFile struct {
	*HPFile
}

func NewTwigMtFile(bufferSize, blockSize int, dirName string) (res TwigMtFile, err error) {
	res.HPFile, err = NewHPFile(bufferSize, blockSize, dirName)
	return
}

/*
We store 1~255, 2048~4095, there levels in the middle are ignored
Level_11  =  1
Level_10  =  2~3
Level_9   =  4~7
Level_8   =  8~15
Level_7   =  16~31
Level_6   =  32~63
Level_5   =  64~127
Level_4   =  128~255
Level_3   X  256~511
Level_2   X  512~1023
Level_1   X  1024~2047
Level_0   =  2048~4095
*/

const (
	IgnoredCount     = 2048 - 256
	TwigMtFullLength = 4095
	TwigMtEntryCount = TwigMtFullLength - IgnoredCount
	TwigMtSize       = 12 + TwigMtEntryCount*32
)

func (tf *TwigMtFile) AppendTwig(mtree [][32]byte, firstEntryPos int64) {
	if firstEntryPos < 0 {
		panic(fmt.Sprintf("Invalid first entry position: %d", firstEntryPos))
	}
	if len(mtree) != TwigMtFullLength {
		panic(fmt.Sprintf("len(mtree) != %d", TwigMtEntryCount))
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(firstEntryPos))
	h := meow.New32(0)
	_, _ = h.Write(buf[:])
	_, err := tf.HPFile.Append([][]byte{buf[:], h.Sum(nil)}) // 8+4 bytes
	if err != nil {
		panic(err)
	}
	for i := 1; i < 256; i++ {
		_, err := tf.HPFile.Append([][]byte{mtree[i-1][:]}) // 32 bytes
		if err != nil {
			panic(err)
		}
	}
	// the nodes in the ignore range won't be saved into disk
	for i := 2048; i <= len(mtree); i++ {
		_, err := tf.HPFile.Append([][]byte{mtree[i-1][:]}) // 32 bytes
		if err != nil {
			panic(err)
		}
	}
}

func (tf *TwigMtFile) GetFirstEntryPos(twigID int64) int64 {
	var buf [12]byte
	err := tf.HPFile.ReadAt(buf[:], twigID*TwigMtSize, false)
	if err != nil {
		panic(err)
	}
	h := meow.New32(0)
	_, _ = h.Write(buf[:8])
	if !bytes.Equal(buf[8:], h.Sum(nil)) {
		panic("Checksum Error!")
	}
	return int64(binary.LittleEndian.Uint64(buf[:8]))
}

func getLeafRange(hashID int) (start, end int) {
	if 256 <= hashID && hashID < 512 {
		return hashID * 8, hashID*8 + 8
	} else if hashID < 1024 {
		return (hashID / 2) * 8, (hashID/2)*8 + 8
	} else if hashID < 2048 {
		return (hashID / 4) * 8, (hashID/4)*8 + 8
	} else {
		panic("Invalid hashID")
	}
}

func (tf *TwigMtFile) getHashNodeInIgnoreRange(twigID int64, hashID int, cache map[int][]byte) []byte {
	start, end := getLeafRange(hashID)
	var buf [32 * 8]byte
	offset := twigID*int64(TwigMtSize) + 12 + (int64(start)-1)*32 - IgnoredCount*32
	err := tf.HPFile.ReadAt(buf[:], offset, false)
	if err != nil { // Cannot read them in one call because of file-crossing
		for i := 0; i < 8; i++ { //read them in 8 steps in case of file-crossing
			err := tf.HPFile.ReadAt(buf[i*32:i*32+32], offset+int64(i*32), false)
			if err != nil {
				panic(err)
			}
		}
	}
	// recover a little cone with 8 leaves into cache
	level := byte(0)
	for id := start / 2; id < end/2; id++ {
		off := (id - start/2) * 64
		cache[id] = hash2(level, buf[off:off+32], buf[off+32:off+64])
	}
	level = byte(1)
	for id := start / 4; id < end/4; id++ {
		cache[id] = hash2(level, cache[id*2], cache[id*2+1])
	}
	level = byte(2)
	id := start / 8
	cache[id] = hash2(level, cache[id*2], cache[id*2+1])
	return cache[hashID] // we are sure the hashID is in this little cone
}

func (tf *TwigMtFile) GetHashNode(twigID int64, hashID int, cache map[int][]byte) []byte {
	var buf [32]byte
	if hashID <= 0 || hashID >= 4096 {
		panic(fmt.Sprintf("Invalid hashID: %d", hashID))
	}
	if 256 <= hashID && hashID < 2048 {
		return tf.getHashNodeInIgnoreRange(twigID, hashID, cache)
	}
	offset := twigID*int64(TwigMtSize) + 12 + (int64(hashID)-1)*32
	if hashID >= 2048 {
		offset = offset - IgnoredCount*32
	}
	err := tf.HPFile.ReadAt(buf[:], offset, false)
	if err != nil {
		panic(err)
	}
	return buf[:]
}

func (tf *TwigMtFile) Size() int64 {
	return tf.HPFile.Size()
}
func (tf *TwigMtFile) Truncate(size int64) {
	err := tf.HPFile.Truncate(size)
	if err != nil {
		panic(err)
	}
	//return
}
func (tf *TwigMtFile) Flush() {
	tf.HPFile.Flush()
}
func (tf *TwigMtFile) WaitForFlushing() {
	tf.HPFile.WaitForFlushing()
}
func (tf *TwigMtFile) StartFlushing() {
	tf.HPFile.StartFlushing()
}
func (tf *TwigMtFile) Close() {
	err := tf.HPFile.Close()
	if err != nil {
		panic(err)
	}
}
func (tf *TwigMtFile) PruneHead(off int64) {
	err := tf.HPFile.PruneHead(off)
	if err != nil {
		panic(err)
	}
}
