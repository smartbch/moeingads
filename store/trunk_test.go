package store

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/smartbch/moeingads"
	"github.com/smartbch/moeingads/store/types"
)

type Coord struct {
	x, y uint32
}

func (coord *Coord) ToBytes() []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[:4], coord.x)
	binary.LittleEndian.PutUint32(buf[4:], coord.y)
	return buf[:]
}

func (coord *Coord) FromBytes(buf []byte) {
	if len(buf) != 8 {
		panic("length is not 8")
	}
	coord.x = binary.LittleEndian.Uint32(buf[:4])
	coord.y = binary.LittleEndian.Uint32(buf[4:])
}

func (coord *Coord) DeepCopy() interface{} {
	return &Coord{
		x: coord.x,
		y: coord.y,
	}
}

type TestOp struct {
	isDel  bool
	ignore bool
	key    []byte
	value  []byte
	obj    *Coord
}

func runList(ts *TrunkStore, opList []TestOp) {
	for _, op := range opList {
		if op.isDel {
			ts.PrepareForDeletion(op.key)
		} else {
			ts.PrepareForUpdate(op.key)
		}
	}
	ts.Update(func(db types.SetDeleter) {
		for _, op := range opList {
			if op.ignore {
				continue
			}
			if op.isDel {
				db.Delete(op.key)
			} else if op.obj != nil {
				db.Set(op.key, op.obj.ToBytes())
			} else {
				db.Set(op.key, op.value)
			}
		}
	})
}

func getListAdd() []TestOp {
	return []TestOp{
		{isDel: false, ignore: false, key: []byte("00003210"), value: []byte("00"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00003211"), value: []byte("10"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00003212"), value: []byte("20"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00003213"), value: []byte{3, 0, 0, 0, 1, 0, 0, 0}, obj: nil},
		{isDel: false, ignore: false, key: []byte("00003214"), value: nil, obj: &Coord{x: 4, y: 4}},
		{isDel: false, ignore: false, key: []byte("00003215"), value: []byte("50"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00003216"), value: []byte("60"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00003217"), value: []byte("70"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00043218"), value: []byte("80"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00043219"), value: []byte("90"), obj: nil},
		{isDel: false, ignore: false, key: []byte("0004321a"), value: []byte{1, 0, 0, 0, 3, 0, 0, 0}, obj: nil},
		{isDel: false, ignore: false, key: []byte("0004321b"), value: nil, obj: &Coord{x: 8, y: 8}},
		{isDel: false, ignore: false, key: []byte("0004321c"), value: []byte("c0"), obj: nil},
		{isDel: false, ignore: false, key: []byte("0004321d"), value: []byte("d0"), obj: nil},
		{isDel: false, ignore: false, key: []byte("0004321e"), value: nil, obj: &Coord{x: 3, y: 3}},
		{isDel: false, ignore: false, key: []byte("0004321f"), value: []byte("f0"), obj: nil},
	}
}

func getListDel() []TestOp {
	return []TestOp{
		{isDel: true, ignore: true, key: []byte("00003210"), value: []byte("00"), obj: nil},
		{isDel: true, ignore: false, key: []byte("00003211"), value: []byte("10"), obj: nil},
		{isDel: true, ignore: false, key: []byte("00003212"), value: []byte("20"), obj: nil},
		{isDel: true, ignore: false, key: []byte("00003217"), value: []byte("70"), obj: nil},
		{isDel: false, ignore: false, key: []byte("00043218"), value: []byte{88, 0, 0, 0, 88, 0, 0, 0}, obj: nil},
		{isDel: true, ignore: true, key: []byte("0004321b"), value: nil, obj: &Coord{x: 8, y: 8}},
		{isDel: true, ignore: false, key: []byte("0004321e"), value: nil, obj: nil},
	}
}

func TestTrunk(t *testing.T) {
	first := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	last := []byte{255, 255, 255, 255, 255, 255, 255, 255}
	mads := moeingads.NewMoeingADS4Mock([][]byte{first, last})

	root := NewRootStore(mads, func(k []byte) bool {
		return k[0] != byte('0')
	})
	root.SetHeight(1)
	ts := root.GetTrunkStore(1000).(*TrunkStore)

	list1 := getListAdd()
	runList(ts, list1)

	var check1 = func() {
		assert.Equal(t, []byte("00"), ts.Get([]byte("00003210")))
		assert.Equal(t, []byte("10"), ts.Get([]byte("00003211")))
		assert.Equal(t, []byte("20"), ts.Get([]byte("00003212")))
		assert.Equal(t, []byte{4, 0, 0, 0, 4, 0, 0, 0}, ts.Get([]byte("00003214")))
		assert.Equal(t, []byte("50"), ts.Get([]byte("00003215")))
		assert.Equal(t, []byte("60"), ts.Get([]byte("00003216")))
		assert.Equal(t, []byte("70"), ts.Get([]byte("00003217")))
		assert.Equal(t, []byte("80"), ts.Get([]byte("00043218")))
		assert.Equal(t, []byte("90"), ts.Get([]byte("00043219")))
		assert.Equal(t, []byte{8, 0, 0, 0, 8, 0, 0, 0}, ts.Get([]byte("0004321b")))
		assert.Equal(t, []byte("c0"), ts.Get([]byte("0004321c")))
		assert.Equal(t, []byte("d0"), ts.Get([]byte("0004321d")))
		assert.Equal(t, []byte("f0"), ts.Get([]byte("0004321f")))

	}

	check1()
	ts.Close(true)

	root.SetHeight(2)
	ts = root.GetTrunkStore(1000).(*TrunkStore)
	check1()

	//=========
	list2 := getListDel()
	runList(ts, list2)

	var check2 = func() {
		assert.Equal(t, []byte("00"), ts.Get([]byte("00003210")))
		assert.Nil(t, ts.Get([]byte("00003217")))
		assert.Equal(t, []byte{88, 0, 0, 0, 88, 0, 0, 0}, ts.Get([]byte("00043218")))

	}

	check2()
	ts.Close(true)

	root.SetHeight(3)
	ts = root.GetTrunkStore(1000).(*TrunkStore)
	check2()

	ts.Close(false)

	mads.Close()
	os.RemoveAll("./rocksdb.db")
}
