package indextree

import (
	"bytes"
	"io"

	"github.com/moeing-chain/MoeingADS/indextree/b"
)

type MockIndexTree struct {
	bt *b.Tree
}

func NewMockIndexTree() *MockIndexTree {
	return &MockIndexTree{
		bt: b.TreeNew(bytes.Compare),
	}
}

func (it *MockIndexTree) ActiveCount() int {
	return it.bt.Len()
}

func (it *MockIndexTree) Init(repFn func([]byte)) error {
	return nil
}

func (it *MockIndexTree) BeginWrite(height int64) {
	return
}

func (it *MockIndexTree) EndWrite() {
	return
}

func (it *MockIndexTree) GetAtHeight(k []byte, height uint64) (uint64, bool) {
	panic("Not Implemented")
}

func (it *MockIndexTree) Get(key []byte) (uint64, bool) {
	return it.bt.Get(key)
}

func (it *MockIndexTree) Set(key []byte, v uint64) {
	it.bt.Set(key, v)
}

func (it *MockIndexTree) Delete(key []byte) {
	it.bt.Delete(key)
}

func (it *MockIndexTree) SetPruneHeight(h uint64) {
	//do nothing
}

func (it *MockIndexTree) Close() {
}

// Create a forward iterator from the B-Tree
func (it *MockIndexTree) Iterator(start, end []byte) Iterator {
	iter := &ForwardIter{start: start, end: end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = it.bt.Seek(start)
	iter.Next() //fill key, value, err
	return iter
}

// Create a backward iterator from the B-Tree
func (it *MockIndexTree) ReverseIterator(start, end []byte) Iterator {
	iter := &BackwardIter{start: start, end: end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	var ok bool
	iter.enumerator, ok = it.bt.Seek(end)
	if ok { // [start, end) end is exclusive
		iter.enumerator.Prev()
	}
	iter.Next() //fill key, value, err
	return iter
}

type ForwardIter struct {
	enumerator *b.Enumerator
	start      []byte
	end        []byte
	key        []byte
	value      uint64
	err        error
}
type BackwardIter struct {
	enumerator *b.Enumerator
	start      []byte
	end        []byte
	key        []byte
	value      uint64
	err        error
}

func (iter *ForwardIter) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *ForwardIter) Valid() bool {
	return iter.err == nil
}
func (iter *ForwardIter) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Next()
		if bytes.Compare(iter.key, iter.end) >= 0 {
			iter.err = io.EOF
		}
	}
}
func (iter *ForwardIter) Key() []byte {
	return iter.key
}
func (iter *ForwardIter) Value() uint64 {
	return iter.value
}
func (iter *ForwardIter) Close() {
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}

func (iter *BackwardIter) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *BackwardIter) Valid() bool {
	return iter.err == nil
}
func (iter *BackwardIter) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Prev()
		if bytes.Compare(iter.key, iter.start) < 0 {
			iter.err = io.EOF
		}
	}
}
func (iter *BackwardIter) Key() []byte {
	return iter.key
}
func (iter *BackwardIter) Value() uint64 {
	return iter.value
}
func (iter *BackwardIter) Close() {
	if iter.enumerator != nil {
		iter.enumerator.Close()
	}
}


