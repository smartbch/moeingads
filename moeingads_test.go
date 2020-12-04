package moeingads

import (
	"fmt"
	"testing"
	"os"

	"github.com/stretchr/testify/assert"

)

type TestOp struct {
	isDel  bool
	ignore bool
	key    []byte
	value  []byte
}

func getListAdd() []TestOp {
	return []TestOp{
		{isDel: false, ignore: false, key: []byte("43210"), value: []byte("00")},
		{isDel: false, ignore: false, key: []byte("43211"), value: []byte("10")},
		{isDel: false, ignore: false, key: []byte("43212"), value: []byte("20")},
		{isDel: false, ignore: false, key: []byte("43213"), value: []byte("30")},
		{isDel: false, ignore: false, key: []byte("43214"), value: []byte("40")},
		{isDel: false, ignore: false, key: []byte("43215"), value: []byte("50")},
		{isDel: false, ignore: false, key: []byte("43216"), value: []byte("60")},
		{isDel: false, ignore: false, key: []byte("43217"), value: []byte("70")},
		{isDel: false, ignore: false, key: []byte("43218"), value: []byte("80")},
		{isDel: false, ignore: false, key: []byte("43219"), value: []byte("90")},
		{isDel: false, ignore: false, key: []byte("4321a"), value: []byte("a0")},
		{isDel: false, ignore: false, key: []byte("4321b"), value: []byte("b0")},
		{isDel: false, ignore: false, key: []byte("4321c"), value: []byte("c0")},
		{isDel: false, ignore: false, key: []byte("4321d"), value: []byte("d0")},
		{isDel: false, ignore: false, key: []byte("4321e"), value: []byte("e0")},
		{isDel: false, ignore: false, key: []byte("4321f"), value: []byte("f0")},
	}
}

func getListModify() []TestOp {
	return []TestOp{
		{isDel: true, ignore: false, key: []byte("43211"), value: []byte("10")}, //effective
		{isDel: true, ignore: true,  key: []byte("43212"), value: []byte("20")},
		{isDel: true, ignore: false, key: []byte("43213"), value: []byte("30")}, //effective
		{isDel: true, ignore: false, key: []byte("43214"), value: []byte("40")}, //effective
		{isDel: true, ignore: false, key: []byte("43215"), value: []byte("50")}, //effective
		{isDel: true, ignore: true,  key: []byte("43216"), value: []byte("60")},
		{isDel: true, ignore: false, key: []byte("43217"), value: []byte("70")}, //effective
		{isDel: true, ignore: true,  key: []byte("43218"), value: []byte("80")},
		{isDel: false, ignore: true, key: []byte("43219"), value: []byte("90")},
		{isDel: true, ignore: true,  key: []byte("4321a"), value: []byte("a0")},
		{isDel: true, ignore: false, key: []byte("4321b"), value: []byte("b0")}, //effective
		{isDel: true, ignore: true,  key: []byte("4321c"), value: []byte("c0")},
		{isDel: true, ignore: false, key: []byte("4321d"), value: []byte("d0")}, //effective
		{isDel: true, ignore: true,  key: []byte("4321e"), value: []byte("e0")},

		{isDel: false, ignore: false, key: []byte("432144"), value: []byte("444")},
		{isDel: false, ignore: false, key: []byte("432155"), value: []byte("555")},
		{isDel: false, ignore: false, key: []byte("432166"), value: []byte("666")},
		{isDel: false, ignore: false, key: []byte("432177"), value: []byte("777")},
		{isDel: false, ignore: false, key: []byte("432188"), value: []byte("888")},
		{isDel: false, ignore: false, key: []byte("4321aa"), value: []byte("aaa")},
		{isDel: false, ignore: false, key: []byte("4321bb"), value: []byte("bbb")},
	}
}

func runList(okv *MoeingADS, opList []TestOp, height int64) {
	for _, op := range opList {
		if op.isDel {
			okv.PrepareForDeletion(op.key)
		} else {
			okv.PrepareForUpdate(op.key)
		}
	}
	okv.BeginWrite(height)
	for _, op := range opList {
		if op.ignore {
			continue
		}
		if op.isDel {
			okv.Delete(op.key)
		} else {
			okv.Set(op.key, op.value)
		}
	}
	okv.EndWrite()
}

func EntryToStr(e *Entry) string {
	return fmt.Sprintf("K:%s nK:%#v(%d) V:%s H:%d LH:%d SN:%d", e.Key, e.NextKey, len(e.NextKey), e.Value, e.Height, e.LastHeight, e.SerialNum)
}

func Test1(t *testing.T) {
	first := []byte{0}
	last := []byte{255,255,255,255,255,255}
	okv := NewMoeingADS4Mock([][]byte{first, last})
	e := okv.GetEntry(first)
	assert.Equal(t, "K:\x00 nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V: H:-1 LH:-1 SN:0", EntryToStr(e))
	e = okv.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V: H:-1 LH:-1 SN:1", EntryToStr(e))


	list1 := getListAdd()
	runList(okv, list1, 0)
	e = okv.GetEntry(first)
	assert.Equal(t, "K:\x00 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x30}(5) V: H:0 LH:-1 SN:2", EntryToStr(e))
	e = okv.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V: H:-1 LH:-1 SN:1", EntryToStr(e))

	resList := []string{
"K:43210 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x31}(5) V:00 H:0 LH:0 SN:3",
"K:43211 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x32}(5) V:10 H:0 LH:0 SN:4",
"K:43212 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x33}(5) V:20 H:0 LH:0 SN:5",
"K:43213 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x34}(5) V:30 H:0 LH:0 SN:6",
"K:43214 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x35}(5) V:40 H:0 LH:0 SN:7",
"K:43215 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x36}(5) V:50 H:0 LH:0 SN:8",
"K:43216 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x37}(5) V:60 H:0 LH:0 SN:9",
"K:43217 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x38}(5) V:70 H:0 LH:0 SN:10",
"K:43218 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x39}(5) V:80 H:0 LH:0 SN:11",
"K:43219 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x61}(5) V:90 H:0 LH:0 SN:12",
"K:4321a nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x62}(5) V:a0 H:0 LH:0 SN:13",
"K:4321b nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x63}(5) V:b0 H:0 LH:0 SN:14",
"K:4321c nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x64}(5) V:c0 H:0 LH:0 SN:15",
"K:4321d nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x65}(5) V:d0 H:0 LH:0 SN:16",
"K:4321e nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x66}(5) V:e0 H:0 LH:0 SN:17",
"K:4321f nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V:f0 H:0 LH:0 SN:18",
}
	for i, op := range list1 {
		e := okv.GetEntry(op.key)
		assert.Equal(t, resList[i], EntryToStr(e))
	}

	fmt.Printf("===========================\n")
	findIt := okv.PrepareForDeletion([]byte("1234"))
	assert.Equal(t, false, findIt)
	list2 := getListModify()
	runList(okv, list2, 1)
	e = okv.GetEntry(first)
	assert.Equal(t, "K:\x00 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x30}(5) V: H:0 LH:-1 SN:2", EntryToStr(e))
	e = okv.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V: H:-1 LH:-1 SN:1", EntryToStr(e))
	e = okv.GetEntry([]byte("43210"))
	assert.Equal(t, "K:43210 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x32}(5) V:00 H:1 LH:0 SN:19", EntryToStr(e))
	e = okv.GetEntry([]byte("43211"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("43212"))
	assert.Equal(t, "K:43212 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x34, 0x34}(6) V:20 H:1 LH:0 SN:20", EntryToStr(e))
	e = okv.GetEntry([]byte("43213"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("43214"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("432144"))
	assert.Equal(t, "K:432144 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x35, 0x35}(6) V:444 H:1 LH:0 SN:21", EntryToStr(e))
	e = okv.GetEntry([]byte("43215"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("432155"))
	assert.Equal(t, "K:432155 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x36}(5) V:555 H:1 LH:0 SN:22", EntryToStr(e))
	e = okv.GetEntry([]byte("43216"))
	assert.Equal(t, "K:43216 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x36, 0x36}(6) V:60 H:1 LH:0 SN:23", EntryToStr(e))
	e = okv.GetEntry([]byte("432166"))
	assert.Equal(t, "K:432166 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x37, 0x37}(6) V:666 H:1 LH:0 SN:24", EntryToStr(e))
	e = okv.GetEntry([]byte("43217"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("432177"))
	assert.Equal(t, "K:432177 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x38}(5) V:777 H:1 LH:0 SN:25", EntryToStr(e))
	e = okv.GetEntry([]byte("43218"))
	assert.Equal(t, "K:43218 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x38, 0x38}(6) V:80 H:1 LH:0 SN:26", EntryToStr(e))
	e = okv.GetEntry([]byte("432188"))
	assert.Equal(t, "K:432188 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x39}(5) V:888 H:1 LH:0 SN:27", EntryToStr(e))
	e = okv.GetEntry([]byte("43219"))
	assert.Equal(t, "K:43219 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x61}(5) V:90 H:0 LH:0 SN:12", EntryToStr(e))
	e = okv.GetEntry([]byte("4321a"))
	assert.Equal(t, "K:4321a nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x61, 0x61}(6) V:a0 H:1 LH:0 SN:28", EntryToStr(e))
	e = okv.GetEntry([]byte("4321aa"))
	assert.Equal(t, "K:4321aa nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x62, 0x62}(6) V:aaa H:1 LH:0 SN:29", EntryToStr(e))
	e = okv.GetEntry([]byte("4321b"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("4321bb"))
	assert.Equal(t, "K:4321bb nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x63}(5) V:bbb H:1 LH:0 SN:30", EntryToStr(e))
	e = okv.GetEntry([]byte("4321c"))
	assert.Equal(t, "K:4321c nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x65}(5) V:c0 H:1 LH:0 SN:31", EntryToStr(e))
	e = okv.GetEntry([]byte("4321d"))
	assert.Nil(t, e)
	e = okv.GetEntry([]byte("4321e"))
	assert.Equal(t, "K:4321e nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x66}(5) V:e0 H:0 LH:0 SN:17", EntryToStr(e))
	e = okv.GetEntry([]byte("4321f"))
	assert.Equal(t, "K:4321f nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(6) V:f0 H:0 LH:0 SN:18", EntryToStr(e))

	iter := okv.Iterator([]byte("4321c"), []byte("4321f"))
	start, end := iter.Domain()
	assert.Equal(t, []byte("4321c"), start)
	assert.Equal(t, []byte("4321f"), end)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, start, iter.Key())
	assert.Equal(t, []byte("c0"), iter.Value())
	iter.Next()
	assert.Equal(t, []byte("e0"), iter.Value())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	assert.Nil(t, iter.Value())
	iter.Close()

	e = okv.GetEntry([]byte("432177"))
	assert.Equal(t, "K:432177 nK:[]byte{0x34, 0x33, 0x32, 0x31, 0x38}(5) V:777 H:1 LH:0 SN:25", EntryToStr(e))
	e = okv.GetEntry([]byte("43218"))
	iter = okv.ReverseIterator([]byte("432177"), []byte("43218"))
	assert.Equal(t, true, iter.Valid())
	start, _ = iter.Domain()
	assert.Equal(t, start, iter.Key())
	assert.Equal(t, []byte("777"), iter.Value())
	iter.Next()

	okv.Close()
	os.RemoveAll("./rocksdb.db")
}

