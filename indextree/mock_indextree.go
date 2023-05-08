package indextree

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/smartbch/moeingads/indextree/b"
)

type MockIndexTree struct {
	bt *b.Tree
}

func NewMockIndexTree() *MockIndexTree {
	return &MockIndexTree{
		bt: b.TreeNew(),
	}
}

func (tree *MockIndexTree) SetDuringInit(b bool) {
}

func (it *MockIndexTree) ActiveCount() int {
	return it.bt.Len()
}

func (it *MockIndexTree) Init(repFn func([]byte)) error {
	return nil
}

func (it *MockIndexTree) BeginWrite(height int64) {
	//return
}

func (it *MockIndexTree) EndWrite() {
	//return
}

func (it *MockIndexTree) GetAtHeight(k []byte, height uint64) (int64, bool) {
	panic("Not Implemented")
}

func (it *MockIndexTree) Get(key []byte) (int64, bool) {
	return it.bt.Get(binary.BigEndian.Uint64(key))
}

func (it *MockIndexTree) Set(key []byte, v int64) {
	it.bt.Set(binary.BigEndian.Uint64(key), v)
}

func (it *MockIndexTree) Delete(key []byte) {
	it.bt.Delete(binary.BigEndian.Uint64(key))
}

func (it *MockIndexTree) SetPruneHeight(h uint64) {
	//do nothing
}

func (it *MockIndexTree) Close() {
}

// Create a forward iterator from the B-Tree
func (it *MockIndexTree) Iterator(start, end []byte) Iterator {
	iter := &ForwardIter{
		start: binary.BigEndian.Uint64(start),
		end:   binary.BigEndian.Uint64(end),
	}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = it.bt.Seek(iter.start)
	iter.Next() //fill key, value, err
	return iter
}

// Create a backward iterator from the B-Tree
func (it *MockIndexTree) ReverseIterator(start, end []byte) Iterator {
	iter := &BackwardIter{
		start: binary.BigEndian.Uint64(start),
		end:   binary.BigEndian.Uint64(end),
	}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	var ok bool
	iter.enumerator, ok = it.bt.Seek(iter.end)
	if ok { // [start, end) end is exclusive
		_, _, _ = iter.enumerator.Prev()
	}
	iter.Next() //fill key, value, err
	return iter
}

type ForwardIter struct {
	enumerator *b.Enumerator
	start      uint64
	end        uint64
	key        uint64
	value      int64
	err        error
}
type BackwardIter struct {
	enumerator *b.Enumerator
	start      uint64
	end        uint64
	key        uint64
	value      int64
	err        error
}

func (iter *ForwardIter) Domain() ([]byte, []byte) {
	return u64ToBz(iter.start), u64ToBz(iter.end)
}
func (iter *ForwardIter) Valid() bool {
	return iter.err == nil
}
func (iter *ForwardIter) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Next()
		if iter.key >= iter.end {
			iter.err = io.EOF
		}
	}
}
func (iter *ForwardIter) Key() []byte {
	return u64ToBz(iter.key)
}
func (iter *ForwardIter) Value() int64 {
	return iter.value
}
func (iter *ForwardIter) Close() {
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}

func (iter *BackwardIter) Domain() ([]byte, []byte) {
	return u64ToBz(iter.start), u64ToBz(iter.end)
}
func (iter *BackwardIter) Valid() bool {
	return iter.err == nil
}
func (iter *BackwardIter) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Prev()
		if iter.key < iter.start {
			iter.err = io.EOF
		}
	}
}
func (iter *BackwardIter) Key() []byte {
	return u64ToBz(iter.key)
}
func (iter *BackwardIter) Value() int64 {
	return iter.value
}
func (iter *BackwardIter) Close() {
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}
