package moeingads

import (
	"fmt"
	"os"
	"testing"

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
		{isDel: false, ignore: false, key: []byte("00432100"), value: []byte("00")},
		{isDel: false, ignore: false, key: []byte("00432110"), value: []byte("10")},
		{isDel: false, ignore: false, key: []byte("00432120"), value: []byte("20")},
		{isDel: false, ignore: false, key: []byte("00432130"), value: []byte("30")},
		{isDel: false, ignore: false, key: []byte("00432140"), value: []byte("40")},
		{isDel: false, ignore: false, key: []byte("00432150"), value: []byte("50")},
		{isDel: false, ignore: false, key: []byte("00432160"), value: []byte("60")},
		{isDel: false, ignore: false, key: []byte("00432170"), value: []byte("70")},
		{isDel: false, ignore: false, key: []byte("00432180"), value: []byte("80")},
		{isDel: false, ignore: false, key: []byte("00432190"), value: []byte("90")},
		{isDel: false, ignore: false, key: []byte("004321a0"), value: []byte("a0")},
		{isDel: false, ignore: false, key: []byte("004321b0"), value: []byte("b0")},
		{isDel: false, ignore: false, key: []byte("004321c0"), value: []byte("c0")},
		{isDel: false, ignore: false, key: []byte("004321d0"), value: []byte("d0")},
		{isDel: false, ignore: false, key: []byte("004321e0"), value: []byte("e0")},
		{isDel: false, ignore: false, key: []byte("004321f0"), value: []byte("f0")},
	}
}

func getListModify() []TestOp {
	return []TestOp{
		{isDel: true, ignore: false, key: []byte("00432110"), value: []byte("10")}, //effective
		{isDel: true, ignore: true, key: []byte("00432120"), value: []byte("20")},
		{isDel: true, ignore: false, key: []byte("00432130"), value: []byte("30")}, //effective
		{isDel: true, ignore: false, key: []byte("00432140"), value: []byte("40")}, //effective
		{isDel: true, ignore: false, key: []byte("00432150"), value: []byte("50")}, //effective
		{isDel: true, ignore: true, key: []byte("00432160"), value: []byte("60")},
		{isDel: true, ignore: false, key: []byte("00432170"), value: []byte("70")}, //effective
		{isDel: true, ignore: true, key: []byte("00432180"), value: []byte("80")},
		{isDel: false, ignore: true, key: []byte("00432190"), value: []byte("90")},
		{isDel: true, ignore: true, key: []byte("004321a0"), value: []byte("a0")},
		{isDel: true, ignore: false, key: []byte("004321b0"), value: []byte("b0")}, //effective
		{isDel: true, ignore: true, key: []byte("004321c0"), value: []byte("c0")},
		{isDel: true, ignore: false, key: []byte("004321d0"), value: []byte("d0")}, //effective
		{isDel: true, ignore: true, key: []byte("004321e0"), value: []byte("e0")},

		{isDel: false, ignore: false, key: []byte("00432144"), value: []byte("444")},
		{isDel: false, ignore: false, key: []byte("00432155"), value: []byte("555")},
		{isDel: false, ignore: false, key: []byte("00432166"), value: []byte("666")},
		{isDel: false, ignore: false, key: []byte("00432177"), value: []byte("777")},
		{isDel: false, ignore: false, key: []byte("00432188"), value: []byte("888")},
		{isDel: false, ignore: false, key: []byte("004321aa"), value: []byte("aaa")},
		{isDel: false, ignore: false, key: []byte("004321bb"), value: []byte("bbb")},
	}
}

func runList(ads *MoeingADS, opList []TestOp, height int64) {
	for _, op := range opList {
		if op.isDel {
			ads.PrepareForDeletion(op.key)
		} else {
			ads.PrepareForUpdate(op.key)
		}
	}
	ads.BeginWrite(height)
	for _, op := range opList {
		if op.ignore {
			continue
		}
		if op.isDel {
			ads.Delete(op.key)
		} else {
			ads.Set(op.key, op.value)
		}
	}
	ads.EndWrite()
}

func EntryToStr(e *Entry) string {
	return fmt.Sprintf("K:%s nK:%#v(%d) V:%s H:%d LH:%d SN:%d", e.Key, e.NextKey, len(e.NextKey), e.Value, e.Height, e.LastHeight, e.SerialNum)
}

func Test1(t *testing.T) {
	first := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	last := []byte{255, 255, 255, 255, 255, 255, 255, 255}
	ads := NewMoeingADS4Mock([][]byte{first, last})
	e := ads.GetEntry(first)
	assert.Equal(t, "K:\x00\x00\x00\x00\x00\x00\x00\x00 nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V: H:-1 LH:-1 SN:0", EntryToStr(e))
	e = ads.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V: H:-1 LH:-1 SN:0", EntryToStr(e))

	list1 := getListAdd()
	runList(ads, list1, 0)
	e = ads.GetEntry(first)
	assert.Equal(t, "K:\x00\x00\x00\x00\x00\x00\x00\x00 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x30, 0x30}(8) V: H:0 LH:-1 SN:1", EntryToStr(e))
	e = ads.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V: H:-1 LH:-1 SN:0", EntryToStr(e))

	resList := []string{
		"K:00432100 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x31, 0x30}(8) V:00 H:0 LH:0 SN:2",
		"K:00432110 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x32, 0x30}(8) V:10 H:0 LH:0 SN:3",
		"K:00432120 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x33, 0x30}(8) V:20 H:0 LH:0 SN:4",
		"K:00432130 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x34, 0x30}(8) V:30 H:0 LH:0 SN:5",
		"K:00432140 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x35, 0x30}(8) V:40 H:0 LH:0 SN:6",
		"K:00432150 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x36, 0x30}(8) V:50 H:0 LH:0 SN:7",
		"K:00432160 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x37, 0x30}(8) V:60 H:0 LH:0 SN:8",
		"K:00432170 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x38, 0x30}(8) V:70 H:0 LH:0 SN:9",
		"K:00432180 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x39, 0x30}(8) V:80 H:0 LH:0 SN:10",
		"K:00432190 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x61, 0x30}(8) V:90 H:0 LH:0 SN:11",
		"K:004321a0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x62, 0x30}(8) V:a0 H:0 LH:0 SN:12",
		"K:004321b0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x63, 0x30}(8) V:b0 H:0 LH:0 SN:13",
		"K:004321c0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x64, 0x30}(8) V:c0 H:0 LH:0 SN:14",
		"K:004321d0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x65, 0x30}(8) V:d0 H:0 LH:0 SN:15",
		"K:004321e0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x66, 0x30}(8) V:e0 H:0 LH:0 SN:16",
		"K:004321f0 nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V:f0 H:0 LH:0 SN:17",
	}
	for i, op := range list1 {
		e := ads.GetEntry(op.key)
		assert.Equal(t, resList[i], EntryToStr(e))
	}

	findIt := ads.PrepareForDeletion([]byte("00001234"))
	assert.Equal(t, false, findIt)
	list2 := getListModify()
	runList(ads, list2, 1)
	e = ads.GetEntry(first)
	assert.Equal(t, "K:\x00\x00\x00\x00\x00\x00\x00\x00 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x30, 0x30}(8) V: H:0 LH:-1 SN:1", EntryToStr(e))
	e = ads.GetEntry(last)
	assert.Equal(t, "K:\xff\xff\xff\xff\xff\xff\xff\xff nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V: H:-1 LH:-1 SN:0", EntryToStr(e))
	e = ads.GetEntry([]byte("00432100"))
	assert.Equal(t, "K:00432100 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x32, 0x30}(8) V:00 H:1 LH:0 SN:18", EntryToStr(e))
	e = ads.GetEntry([]byte("00432110"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("00432120"))
	assert.Equal(t, "K:00432120 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x34, 0x34}(8) V:20 H:1 LH:0 SN:19", EntryToStr(e))
	e = ads.GetEntry([]byte("00432130"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("00432140"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("00432144"))
	assert.Equal(t, "K:00432144 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x35, 0x35}(8) V:444 H:1 LH:0 SN:20", EntryToStr(e))
	e = ads.GetEntry([]byte("00432150"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("00432155"))
	assert.Equal(t, "K:00432155 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x36, 0x30}(8) V:555 H:1 LH:0 SN:21", EntryToStr(e))
	e = ads.GetEntry([]byte("00432160"))
	assert.Equal(t, "K:00432160 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x36, 0x36}(8) V:60 H:1 LH:0 SN:22", EntryToStr(e))
	e = ads.GetEntry([]byte("00432166"))
	assert.Equal(t, "K:00432166 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x37, 0x37}(8) V:666 H:1 LH:0 SN:23", EntryToStr(e))
	e = ads.GetEntry([]byte("00432170"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("00432177"))
	assert.Equal(t, "K:00432177 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x38, 0x30}(8) V:777 H:1 LH:0 SN:24", EntryToStr(e))
	e = ads.GetEntry([]byte("00432180"))
	assert.Equal(t, "K:00432180 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x38, 0x38}(8) V:80 H:1 LH:0 SN:25", EntryToStr(e))
	e = ads.GetEntry([]byte("00432188"))
	assert.Equal(t, "K:00432188 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x39, 0x30}(8) V:888 H:1 LH:0 SN:26", EntryToStr(e))
	e = ads.GetEntry([]byte("00432190"))
	assert.Equal(t, "K:00432190 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x61, 0x30}(8) V:90 H:0 LH:0 SN:11", EntryToStr(e))
	e = ads.GetEntry([]byte("004321a0"))
	assert.Equal(t, "K:004321a0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x61, 0x61}(8) V:a0 H:1 LH:0 SN:27", EntryToStr(e))
	e = ads.GetEntry([]byte("004321aa"))
	assert.Equal(t, "K:004321aa nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x62, 0x62}(8) V:aaa H:1 LH:0 SN:28", EntryToStr(e))
	e = ads.GetEntry([]byte("004321b0"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("004321bb"))
	assert.Equal(t, "K:004321bb nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x63, 0x30}(8) V:bbb H:1 LH:0 SN:29", EntryToStr(e))
	e = ads.GetEntry([]byte("004321c0"))
	assert.Equal(t, "K:004321c0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x65, 0x30}(8) V:c0 H:1 LH:0 SN:30", EntryToStr(e))
	e = ads.GetEntry([]byte("004321d0"))
	assert.Nil(t, e)
	e = ads.GetEntry([]byte("004321e0"))
	assert.Equal(t, "K:004321e0 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x66, 0x30}(8) V:e0 H:0 LH:0 SN:16", EntryToStr(e))
	e = ads.GetEntry([]byte("004321f0"))
	assert.Equal(t, "K:004321f0 nK:[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}(8) V:f0 H:0 LH:0 SN:17", EntryToStr(e))

	e = ads.GetEntry([]byte("00432177"))
	assert.Equal(t, "K:00432177 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x38, 0x30}(8) V:777 H:1 LH:0 SN:24", EntryToStr(e))
	e = ads.GetEntry([]byte("00432180"))
	assert.Equal(t, "K:00432180 nK:[]byte{0x30, 0x30, 0x34, 0x33, 0x32, 0x31, 0x38, 0x38}(8) V:80 H:1 LH:0 SN:25", EntryToStr(e))

	ads.Close()
	os.RemoveAll("./rocksdb.db")
}
