package b

import (
	"bytes"
	//"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

//func TreeNew(_ func(a, b []byte) int) *Tree {
//func (tree *Tree) Close() {
//func (tree *Tree) PutNewAndGetOld(key []byte, newV uint64) (uint64, bool) {
//func (tree *Tree) Set(key []byte, value uint64) {
//func (tree *Tree) Delete(key []byte) {
//func (tree *Tree) Get(key []byte) (uint64, bool) {
//func (tree *Tree) Seek(key []byte) (*Enumerator, error) {
//func (tree *Tree) SeekFirst(key []byte) (*Enumerator, error) {
//func (e *Enumerator) Close() {
//func (e *Enumerator) Next() (k []byte, v uint64, err error) {
//func (e *Enumerator) Prev() (k []byte, v uint64, err error) {

func mustGet(t *testing.T, bt *Tree, key []byte) uint64 {
	v, ok := bt.Get(key)
	assert.Equal(t, true, ok)
	return v
}

func TestBTree(t *testing.T) {
	bt := TreeNew(func(a, b []byte) int {return bytes.Compare(a,b)})
	iter, err := bt.SeekFirst()
	assert.Equal(t, io.EOF, err)
	_, ok := bt.Seek([]byte("ABcd3210"))
	assert.Equal(t, false, ok)
	bt.Set([]byte("ABcd3210"), 0)
	bt.Set([]byte("ABcd32100"), 1)
	bt.Set([]byte("ABcd3211"), 2)
	bt.Set([]byte("ABcd3212"), 3)
	bt.Set([]byte("ABd3212"), 4)
	bt.Set([]byte("ABxd3212"), 5)
	bt.Set([]byte("ABxd321222"), 6)
	bt.Set([]byte("ABxd3212x2"), 7)
	bt.Set([]byte("BBxd3212"), 8)
	bt.Delete([]byte("ABxd3212x2"))
	assert.Equal(t, uint64(0), mustGet(t, bt, []byte("ABcd3210")))
	assert.Equal(t, uint64(1), mustGet(t, bt, []byte("ABcd32100")))
	assert.Equal(t, uint64(2), mustGet(t, bt, []byte("ABcd3211")))
	assert.Equal(t, uint64(3), mustGet(t, bt, []byte("ABcd3212")))
	assert.Equal(t, uint64(4), mustGet(t, bt, []byte("ABd3212")))
	assert.Equal(t, uint64(5), mustGet(t, bt, []byte("ABxd3212")))
	assert.Equal(t, uint64(6), mustGet(t, bt, []byte("ABxd321222")))
	assert.Equal(t, uint64(8), mustGet(t, bt, []byte("BBxd3212")))
	assert.Equal(t, 8, bt.Len())
	_, ok = bt.Get([]byte("ABxd3212x2"))
	assert.Equal(t, false, ok)
	old, ok := bt.PutNewAndGetOld([]byte("BBxd3212"), 9)
	assert.Equal(t, uint64(8), old)
	assert.Equal(t, true, ok)
	old, ok = bt.PutNewAndGetOld([]byte("BBxd3214"), 10)
	assert.Equal(t, false, ok)
	assert.Equal(t, uint64(10), mustGet(t, bt, []byte("BBxd3214")))

	iter, err = bt.SeekFirst()
	assert.Equal(t, nil, err)

	k, v, err := iter.Prev()
	assert.Equal(t, []byte("ABcd3210"), k)
	assert.Equal(t, uint64(0), v)
	assert.Equal(t, nil, err)

	_, _, err = iter.Prev()
	assert.Equal(t, io.EOF, err)

	iter.Close()

	iter, ok = bt.Seek([]byte("ABcd32110"))
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Next()
	assert.Equal(t, []byte("ABcd3212"), k) // larger than target
	assert.Equal(t, uint64(3), v)
	assert.Equal(t, nil, err)
	k, v, err = iter.Next()
	assert.Equal(t, []byte("ABd3212"), k)
	assert.Equal(t, uint64(4), v)
	assert.Equal(t, nil, err)
	iter.Close()

	iter, ok = bt.Seek([]byte("ABxd3211"))
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Prev()
	assert.Equal(t, []byte("ABd3212"), k) // larger than target
	assert.Equal(t, uint64(4), v)
	assert.Equal(t, nil, err)
	iter.Close()

	iter, ok = bt.Seek([]byte("DD"))
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Prev()
	assert.Equal(t, []byte("BBxd3214"), k) // larger than target
	assert.Equal(t, uint64(10), v)
	assert.Equal(t, nil, err)
	iter.Close()

	bt.Close()
}

