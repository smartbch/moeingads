package datatree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/smartbch/moeingads/types"
)

type Entry = types.Entry

const MaxEntryBytes int = (1 << 24) - 1

var MagicBytes = [8]byte{255, 254, 253, 252, 252, 253, 254, 255}

//var dbg bool

func DummyEntry(sn int64) *Entry {
	return &Entry{
		Key:        []byte("dummy"),
		Value:      []byte("dummy"),
		NextKey:    []byte("dummy"),
		Height:     -2,
		LastHeight: -2,
		SerialNum:  sn,
	}
}

func NullEntry() Entry {
	return Entry{
		Key:        []byte{},
		Value:      []byte{},
		NextKey:    []byte{},
		Height:     -1,
		LastHeight: -1,
		SerialNum:  -1,
	}
}

// Entry serialization format:
// magicBytes 8-bytes
// 8b snList length
// 24b-totalLength (this length does not include padding, snList and this field itself)
// magicBytesPos(list of 32b-int, -1 for ending), posistions are relative to the end of 32b-totalLength
// normalPayload
// DeactivedSerialNumList (list of 64b-int)
// padding-zero-bytes

const (
	MSB32 = uint32(1 << 31)
)

func PutUint24(b []byte, n uint32) {
	//!! if n == 0 {
	//!! 	panic("here PutUint24")
	//!! }
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	b[2] = byte(n >> 16)
}

func GetUint24(b []byte) (n uint32) {
	n = uint32(b[0])
	n |= uint32(b[1]) << 8
	n |= uint32(b[2]) << 16
	//!! if n == 0 {
	//!! 	panic("here GetUint24")
	//!! }
	return
}

//!! func SkipPosList(bz []byte) []byte {
//!! 	for i := 0; i + 4 < len(bz); i+=4 {
//!! 		if (bz[i]&bz[i+1]&bz[i+2]&bz[i+3]) == 0xFF {
//!! 			return bz[i+4:]
//!! 		}
//!! 	}
//!! 	return nil
//!! }

func ExtractKeyFromRawBytes(b []byte) []byte {
	bb := b[4:]
	if (bb[0] & bb[1] & bb[2] & bb[3]) == 0xFF { // No MagicBytes to recover
		length := int(binary.LittleEndian.Uint32(bb[4:8]))
		return append([]byte{}, bb[8:8+length]...)
	}
	bb = append([]byte{}, b[4:]...)
	n := recoverMagicBytes(bb)
	bb = bb[n:]
	length := int(binary.LittleEndian.Uint32(bb[:4]))
	return append([]byte{}, bb[4:4+length]...)
}

func ExtractSerialNum(entryBz []byte) int64 {
	return int64(binary.LittleEndian.Uint64(entryBz[len(entryBz)-8:]))
}

func UpdateSerialNum(entryBz []byte, sn int64) {
	binary.LittleEndian.PutUint64(entryBz[len(entryBz)-8:], uint64(sn))
}

func SNListToBytes(deactivedSerialNumList []int64) []byte {
	res := make([]byte, len(deactivedSerialNumList)*8)
	i := 0
	for _, sn := range deactivedSerialNumList {
		binary.LittleEndian.PutUint64(res[i:i+8], uint64(sn))
		i += 8
	}
	return res
}

func EntryToBytes(entry Entry, deactivedSerialNumList []int64) []byte {
	length := 4 + 4                                                        // 32b-length and empty magicBytesPos
	length += 4*3 + len(entry.Key) + len(entry.Value) + len(entry.NextKey) // Three strings
	length += 8 * 3                                                        // Three int64
	length += len(deactivedSerialNumList) * 8
	b := make([]byte, length)

	b[0] = byte(len(deactivedSerialNumList))
	const start = 8
	writeEntryPayload(b[start:], entry, deactivedSerialNumList)

	// MagicBytes can not lay in or overlap with these 64b integers
	stop := len(b) - len(deactivedSerialNumList)*8 - 3*8
	magicBytesPosList := getAllPos(b[start:stop], MagicBytes[:])
	if len(magicBytesPosList) == 0 {
		//!! if dbg {
		//!! 	fmt.Printf("here-length %d %d\n", length, length-4-len(deactivedSerialNumList)*8)
		//!! }
		PutUint24(b[1:4], uint32(length-4-len(deactivedSerialNumList)*8))
		binary.LittleEndian.PutUint32(b[4:8], ^uint32(0))
		return b
	}

	// if magicBytesPosList is not empty:
	var zeroBuf [8]byte
	for _, pos := range magicBytesPosList {
		copy(b[start+pos:start+pos+8], zeroBuf[:]) // over-write the occurrence of magic bytes with zeros
	}
	length += 4 * len(magicBytesPosList)
	buf := make([]byte, length)

	bytesAdded := 4 * len(magicBytesPosList)
	var i int
	for i = 0; i < len(magicBytesPosList); i++ {
		pos := magicBytesPosList[i] + bytesAdded /*32b-length*/
		binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], uint32(pos))
	}
	binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], ^uint32(0))
	copy(buf[i*4+8:], b[8:])
	// Re-write the new length. minus 4 because the first 4 bytes of length isn't included
	buf[0] = byte(len(deactivedSerialNumList))
	PutUint24(buf[1:4], uint32(length-4-len(deactivedSerialNumList)*8))
	//!! if dbg {
	//!! 	fmt.Printf("there-length %d %d\n", length, length-4-len(deactivedSerialNumList)*8)
	//!! }
	return buf
}

func writeEntryPayload(b []byte, entry Entry, deactivedSerialNumList []int64) {
	i := 0
	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.Key)))
	i += 4
	copy(b[i:], entry.Key)
	i += len(entry.Key)

	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.Value)))
	i += 4
	copy(b[i:], entry.Value)
	i += len(entry.Value)

	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.NextKey)))
	i += 4
	copy(b[i:], entry.NextKey)
	i += len(entry.NextKey)

	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.Height))
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.LastHeight))
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.SerialNum))
	i += 8

	for _, sn := range deactivedSerialNumList {
		binary.LittleEndian.PutUint64(b[i:i+8], uint64(sn))
		i += 8
	}
}

func getAllPos(s, sep []byte) (allpos []int) {
	for start, pos := 0, 0; start+len(sep) < len(s); start += pos + len(sep) {
		pos = bytes.Index(s[start:], sep)
		if pos == -1 {
			return
		}
		allpos = append(allpos, pos+start)
	}
	return
}

func EntryFromBytes(b []byte, numberOfSN int) (*Entry, []int64) {
	entry := &Entry{}
	i := 0

	length := int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.Key = b[i : i+length]
	i += length

	length = int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.Value = b[i : i+length]
	i += length

	length = int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.NextKey = b[i : i+length]
	i += length

	entry.Height = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8
	entry.LastHeight = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8
	entry.SerialNum = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8

	if numberOfSN == 0 {
		return entry, nil
	}

	deactivedSerialNumList := make([]int64, numberOfSN)
	for j := range deactivedSerialNumList {
		deactivedSerialNumList[j] = int64(binary.LittleEndian.Uint64(b[i : i+8]))
		i += 8
	}

	return entry, deactivedSerialNumList
}

type EntryFile struct {
	*HPFile
}

func getPaddingSize(length int) int {
	rem := length % 8
	if rem == 0 {
		return 0
	} else {
		return 8 - rem
	}
}

func (ef *EntryFile) readMagicBytesAndLength(off int64, withBuf bool) (length int64, numberOfSN int) {
	var buf [12]byte
	err := ef.HPFile.ReadAt(buf[:], off, withBuf)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Now off %d %x %#v %#v\n", off, off, buf[:], MagicBytes[:])
	if !bytes.Equal(buf[:8], MagicBytes[:]) {
		//var buf [120]byte
		//err := ef.HPFile.ReadAt(buf[:], off, withBuf)
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Printf("%#v\n", buf[:])
		panic(fmt.Sprintf("Invalid MagicBytes at %d", off))
	}
	length = int64(GetUint24(buf[9:12]))
	if int(length) >= MaxEntryBytes {
		panic("Entry to long")
	}
	return length, int(buf[8])
}

func getNextPos(off, length int64) int64 {
	length += 8 /*magicbytes*/ + 4 /*length*/
	paddingSize := getPaddingSize(int(length))
	paddedLen := length + int64(paddingSize)
	nextPos := off + paddedLen
	//fmt.Printf("off %d length %d paddingSize %d paddedLen %d nextPos %d\n", off, length, paddingSize, paddedLen, nextPos)
	return nextPos

}

func (ef *EntryFile) ReadEntryAndSNList(off int64) (entry *Entry, deactivedSerialNumList []int64, nextPos int64) {
	entryBz, numberOfSN, nextPos := ef.readEntry(off, true, false, true)
	entry, deactivedSerialNumList = EntryFromBytes(entryBz, numberOfSN)
	return
}

func (ef *EntryFile) ReadEntry(off int64) (entry *Entry, nextPos int64) {
	entryBz, numberOfSN, nextPos := ef.readEntry(off, false, false, false)
	entry, _ = EntryFromBytes(entryBz, numberOfSN)
	return
}

func (ef *EntryFile) ReadEntryRawBytes(off int64) (entryBz []byte, nextPos int64) {
	entryBz, _, nextPos = ef.readEntry(off, false, true, true)
	return
}

func recoverMagicBytes(b []byte) (n int) {
	for n = 0; n+4 < len(b); n += 4 { // recover magic bytes in payload
		pos := binary.LittleEndian.Uint32(b[n : n+4])
		if pos == ^(uint32(0)) {
			n += 4
			break
		}
		if int(pos) >= MaxEntryBytes {
			panic("Position to large")
		}
		copy(b[int(pos)+4:int(pos)+12], MagicBytes[:])
	}
	return
}

func (ef *EntryFile) readEntry(off int64, withSNList, useRaw, withBuf bool) (entrybz []byte, numberOfSN int, nextPos int64) {
	length, numberOfSN := ef.readMagicBytesAndLength(off, withBuf)
	nextPos = getNextPos(off, int64(length)+8*int64(numberOfSN))
	if withSNList {
		length += 8 * int64(numberOfSN) // ignore snlist
	} else {
		numberOfSN = 0
	}
	b := make([]byte, 12+int(length)) // include 12 (magicbytes and length)
	err := ef.HPFile.ReadAt(b, off, withBuf)
	origB := b
	b = b[12:] // ignore magicbytes and length
	if err != nil {
		panic(err)
	}
	if useRaw {
		return origB[8:], numberOfSN, nextPos
	}
	n := recoverMagicBytes(b)
	return b[n:length], numberOfSN, nextPos
}

func NewEntryFile(bufferSize, blockSize int, dirName string) (res EntryFile, err error) {
	res.HPFile, err = NewHPFile(bufferSize, blockSize, dirName)
	res.HPFile.InitPreReader()
	return
}

func (ef *EntryFile) Size() int64 {
	return ef.HPFile.Size()
}
func (ef *EntryFile) Truncate(size int64) {
	err := ef.HPFile.Truncate(size)
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) Flush() {
	ef.HPFile.Flush()
}
func (ef *EntryFile) WaitForFlushing() {
	ef.HPFile.WaitForFlushing()
}
func (ef *EntryFile) StartFlushing() {
	ef.HPFile.StartFlushing()
}
func (ef *EntryFile) Close() {
	err := ef.HPFile.Close()
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) PruneHead(off int64) {
	err := ef.HPFile.PruneHead(off)
	if err != nil {
		panic(err)
	}
}

func (ef *EntryFile) Append(b [2][]byte) (pos int64) {
	//!! if b[0][1] == 0 && b[0][2] == 0  && b[0][3] == 0 {
	//!! 	fmt.Printf("%#v\n", b)
	//!! 	panic("here in Append")
	//!! }
	var bb [4][]byte
	bb[0] = MagicBytes[:]
	bb[1] = b[0]
	bb[2] = b[1]
	paddingSize := getPaddingSize(len(b[0]) + len(b[1]))
	bb[3] = make([]byte, paddingSize) // padding zero bytes
	pos, err := ef.HPFile.Append(bb[:])
	//!! if pos > 108996000 {
	//!! 	dbg = true
	//!! 	fmt.Printf("Append pos %d %#v len(bb[1]) %d padding %d\n", pos, bb[:], len(bb[1]), paddingSize)
	//!! }
	if pos%8 != 0 {
		panic("Entries are not aligned")
	}
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Now Append At: %d len: %d\n", pos, len(b))
	return
}

func (ef *EntryFile) GetActiveEntriesInTwig(twig *Twig) chan []byte {
	return ef.getActiveEntriesInTwig(twig, true)
}

func (ef *EntryFile) getActiveEntriesInTwig(twig *Twig, checkSN bool) chan []byte {
	res := make(chan []byte, 100)
	go func() {
		start := twig.FirstEntryPos
		for i := 0; i < LeafCountInTwig; i++ {
			if twig.getBit(i) {
				entryBz, next := ef.ReadEntryRawBytes(start)
				start = next
				sn := ExtractSerialNum(entryBz)
				if checkSN && (sn%LeafCountInTwig) != int64(i) {
					panic(fmt.Sprintf("mismatch! %d %d %d\n", sn, sn%LeafCountInTwig, i))
				}
				res <- entryBz
			} else { // skip an inactive entry
				length, numberOfSN := ef.readMagicBytesAndLength(start, true)
				start = getNextPos(start, length+8*int64(numberOfSN))
			}
		}
		close(res)
	}()
	return res
}

//!! func (ef *EntryFile) GetActiveEntriesInTwigOld(twig *Twig) chan *Entry {
//!! 	res := make(chan *Entry, 100)
//!! 	go func() {
//!! 		start := twig.FirstEntryPos
//!! 		for i := 0; i < LeafCountInTwig; i++ {
//!! 			if twig.getBit(i) {
//!! 				entry, next := ef.ReadEntry(start)
//!! 				start = next
//!! 				res <- entry
//!! 			} else { // skip an inactive entry
//!! 				length, numberOfSN := ef.readMagicBytesAndLength(start, true)
//!! 				start = getNextPos(start, length+8*int64(numberOfSN))
//!! 			}
//!! 		}
//!! 		close(res)
//!! 	}()
//!! 	return res
//!! }
