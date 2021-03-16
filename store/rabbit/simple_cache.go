package rabbit

import (
	"encoding/binary"

	"github.com/smartbch/MoeingADS/store/types"
)

const (
	EmptyMarkerIndex = 0
	PassbyNumIndex   = 1
	KeyLenStart      = PassbyNumIndex + 4
	KeyStart         = KeyLenStart + 4
)

// When a transaction accesses the underlying store with the 'RabbitJump' algorithm,
// each "rabbit hole" is modeled as a CacheValue.
// A SimpleCacheStore caches all the "rabbit holes" which are read or written during a transaction.
type CachedValue struct {
	isEmpty   bool // this hole has no value, but passbyNum!=0
	passbyNum uint32
	key       []byte
	value     []byte
	// isDeleted and isDirty only exist in memory, they are not stored on disk
	isDeleted bool // this hole is marked delete, the underlying store should also delete it later
	isDirty   bool // this hole is changed, its new content should be written back later
}

func (v *CachedValue) IsEmpty() bool {
	return v.isEmpty
}

func (v *CachedValue) GetKey() []byte {
	return v.key
}

func (v *CachedValue) GetValue() []byte {
	return v.value
}

func (v *CachedValue) SetValue(bz []byte) {
	v.value = bz
}

func (v *CachedValue) ToBytes() []byte {
	var buf, value []byte
	if v.isEmpty { //value is not stored when empty
		buf = make([]byte, 1+4+4, 1+4+4+len(v.key))
		buf[EmptyMarkerIndex] = 1
	} else {
		value = v.GetValue()
		buf = make([]byte, 1+4+4, 1+4+4+len(v.key)+len(value))
		buf[EmptyMarkerIndex] = 0
	}
	binary.LittleEndian.PutUint32(buf[PassbyNumIndex:PassbyNumIndex+4], v.passbyNum)
	binary.LittleEndian.PutUint32(buf[KeyLenStart:KeyStart], uint32(len(v.key)))
	buf = append(buf, v.key...)
	if !v.isEmpty {
		buf = append(buf, value...)
	}
	return buf
}

func BytesToCachedValue(buf []byte) *CachedValue {
	if len(buf) < KeyStart {
		return nil
	}
	keyLen := int(binary.LittleEndian.Uint32(buf[KeyLenStart:KeyStart]))
	if len(buf) < KeyStart+keyLen {
		return nil
	}
	res := &CachedValue{
		passbyNum: binary.LittleEndian.Uint32(buf[PassbyNumIndex : PassbyNumIndex+4]),
		key:       buf[KeyStart : KeyStart+keyLen], //use buf directly, not copied
	}
	if buf[EmptyMarkerIndex] != 0 {
		res.isEmpty = true
		res.value = nil
	} else {
		res.isEmpty = false
		res.value = buf[KeyStart+keyLen:] //use buf directly, not copied
	}
	return res
}

type SimpleCacheStore struct {
	m map[[KeySize]byte]*CachedValue
}

func NewSimpleCacheStore() *SimpleCacheStore {
	return &SimpleCacheStore{
		m: make(map[[KeySize]byte]*CachedValue),
	}
}

func (scs *SimpleCacheStore) ScanAllShortKeys(fn func(key [KeySize]byte, dirty bool) bool) {
	for key, cv := range scs.m {
		stop := fn(key, cv.isDirty)
		if stop {
			break
		}
	}
}

func (scs *SimpleCacheStore) ScanAllDirtyEntries(fn func(key, value []byte, isDeleted bool)) {
	for key, cv := range scs.m {
		if !cv.isDirty {
			continue
		}
		fn(key[:], cv.ToBytes(), cv.isDeleted)
	}
}

func (scs *SimpleCacheStore) GetValue(key [KeySize]byte) (value *CachedValue, status types.CacheStatus) {
	v, ok := scs.m[key]
	if !ok {
		status = types.Missed
	} else if v.isDeleted {
		status = types.JustDeleted
	} else {
		value = v
		status = types.Hit
	}
	return
}

func (scs *SimpleCacheStore) SetValue(key [KeySize]byte, value *CachedValue) {
	scs.m[key] = value
}
