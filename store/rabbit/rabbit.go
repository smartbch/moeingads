package rabbit

import (
	"bytes"
	"fmt"

	sha256 "github.com/minio/sha256-simd"

	"github.com/moeing-chain/MoeingADS/store/types"
)

const (
	KeySize = 8 // 2 for fuzz, 8 for production

	NotFound  = 0
	EmptySlot = 1
	Exists    = 2

	MaxFindDepth = 100
)

// RabbitStore inherits SimpleMultiStore to implement the 'RabbitJump' algorithm
type RabbitStore struct {
	readOnly bool
	sms      SimpleMultiStore
}

func NewRabbitStore(parent types.BaseStoreI) (rabbit RabbitStore) {
	rabbit.sms = NewSimpleMultiStore(parent)
	rabbit.readOnly = false
	return
}

func NewReadOnlyRabbitStore(parent types.BaseStoreI) (rabbit RabbitStore) {
	rabbit.sms = NewSimpleMultiStore(parent)
	rabbit.readOnly = true
	return
}

func (rabbit *RabbitStore) GetBaseStore() types.BaseStoreI {
	return rabbit.sms.parent
}

func (rabbit *RabbitStore) IsClean() bool {
	return len(rabbit.sms.cache.m) == 0
}

func (rabbit *RabbitStore) ScanAllShortKeys(fn func(key [KeySize]byte, dirty bool) bool) {
	rabbit.sms.cache.ScanAllShortKeys(fn)
}

func (rabbit *RabbitStore) Has(key []byte) bool {
	_, _, status := rabbit.find(key, true)
	return status == Exists
}

func (rabbit *RabbitStore) GetShortKeyPath(key []byte) (path [][KeySize]byte, ok bool) {
	_, path, status := rabbit.find(key, true)
	ok = status == Exists
	return
}

func (rabbit *RabbitStore) Get(key []byte) []byte {
	cv, _, status := rabbit.find(key, true)
	if status != Exists {
		return nil
	}
	return cv.value
}

func (rabbit *RabbitStore) find(key []byte, earlyExit bool) (cv *CachedValue, path [][KeySize]byte, status int) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	status = NotFound
	for i := 0; i < MaxFindDepth; i++ {
		copy(k[:], hash[:])
		k[0] = k[0] | 0x1 // force the MSB to 1
		path = append(path, k)
		cv = rabbit.sms.GetCachedValue(k)
		if cv == nil {
			return
		}
		if bytes.Equal(cv.key, key) {
			status = Exists
			if cv.isEmpty {
				status = EmptySlot
			}
			return
		} else if earlyExit && cv.passbyNum == 0 {
			return
		} else {
			hash = sha256.Sum256(hash[:])
		}
	}
	panic(fmt.Sprintf("MaxFindDepth(%d) reached!", MaxFindDepth))
}

func (rabbit *RabbitStore) Set(key []byte, bz []byte) {
	rabbit.setHelper(key, bz)
}

func (rabbit *RabbitStore) setHelper(key, value []byte) {
	_, path, status := rabbit.find(key, false)
	if status == Exists { //change
		cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
		cv.value = append([]byte{}, value...)
		rabbit.sms.SetCachedValue(path[len(path)-1], cv)
		return
	}
	if status == EmptySlot { //overwrite
		cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
		cv.key = append([]byte{}, key...)
		cv.value = append([]byte{}, value...)
		cv.isEmpty = false
		rabbit.sms.SetCachedValue(path[len(path)-1], cv)
	} else { //insert
		rabbit.sms.SetCachedValue(path[len(path)-1], &CachedValue{
			key:       append([]byte{}, key...),
			value:     append([]byte{}, value...),
			passbyNum: 0,
			isEmpty:   false,
		})
	}
	// incr passbyNum
	for _, k := range path[:len(path)-1] {
		cv := rabbit.sms.MustGetCachedValue(k)
		cv.passbyNum++
		rabbit.sms.SetCachedValue(k, cv)
	}
}

func (rabbit *RabbitStore) Delete(key []byte) {
	_, path, status := rabbit.find(key, true)
	if status != Exists {
		return
	}
	cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
	if cv.passbyNum == 0 { // can delete it
		cv.isDeleted = true
	} else { // can not delete it, just mark it as deleted
		cv.isEmpty = true
	}
	rabbit.sms.SetCachedValue(path[len(path)-1], cv)
	for _, k := range path[:len(path)-1] {
		cv := rabbit.sms.MustGetCachedValue(k)
		cv.passbyNum--
		if cv.passbyNum == 0 && cv.isEmpty {
			cv.isDeleted = true
		}
		rabbit.sms.SetCachedValue(k, cv)
	}
}

func (rabbit *RabbitStore) Close() {
	rabbit.sms.Close()
}

func (rabbit *RabbitStore) WriteBack() {
	if rabbit.readOnly {
		panic("Cannot writeback a readonly RabbitStore")
	}
	rabbit.sms.WriteBack()
}

func (rabbit *RabbitStore) CloseAndWriteBack(dirty bool) {
	rabbit.sms.Close()
	if dirty {
		rabbit.sms.WriteBack()
	}
}

func (rabbit *RabbitStore) ActiveCount() int {
	return rabbit.sms.parent.ActiveCount()
}
