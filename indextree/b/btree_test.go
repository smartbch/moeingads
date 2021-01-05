package b

import (
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

func mustGet(t *testing.T, bt *Tree, key uint64) int64 {
	v, ok := bt.Get(key)
	assert.Equal(t, true, ok)
	return v
}

func TestBTree(t *testing.T) {
	bt := TreeNew()
	_, err := bt.SeekFirst()
	assert.Equal(t, io.EOF, err)
	_, ok := bt.Seek(0xABcd321000)
	assert.Equal(t, false, ok)
	bt.Set(0xABcd321000, 0)
	bt.Set(0xABcd321010, 1)
	bt.Set(0xABcd321100, 2)
	bt.Set(0xABcd321200, 3)
	bt.Set(0xABd3212000, 4)
	bt.Set(0xABed321200, 5)
	bt.Set(0xABed321222, 6)
	bt.Set(0xABed321232, 7)
	bt.Set(0xBBed321200, 8)
	bt.Delete(0xABed321232)
	assert.Equal(t, int64(0), mustGet(t, bt, 0xABcd321000))
	assert.Equal(t, int64(1), mustGet(t, bt, 0xABcd321010))
	assert.Equal(t, int64(2), mustGet(t, bt, 0xABcd321100))
	assert.Equal(t, int64(3), mustGet(t, bt, 0xABcd321200))
	assert.Equal(t, int64(4), mustGet(t, bt, 0xABd3212000))
	assert.Equal(t, int64(5), mustGet(t, bt, 0xABed321200))
	assert.Equal(t, int64(6), mustGet(t, bt, 0xABed321222))
	assert.Equal(t, int64(8), mustGet(t, bt, 0xBBed321200))
	assert.Equal(t, 8, bt.Len())
	_, ok = bt.Get(0xABed321232)
	assert.Equal(t, false, ok)
	old, ok := bt.PutNewAndGetOld(0xBBed321200, 9)
	assert.Equal(t, int64(8), old)
	assert.Equal(t, true, ok)
	old, ok = bt.PutNewAndGetOld(0xBBed321400, 10)
	assert.Equal(t, false, ok)
	assert.Equal(t, int64(10), mustGet(t, bt, 0xBBed321400))

	iter, err := bt.SeekFirst()
	assert.Equal(t, nil, err)

	k, v, err := iter.Prev()
	assert.Equal(t, uint64(0xABcd321000), k)
	assert.Equal(t, int64(0), v)
	assert.Equal(t, nil, err)

	_, _, err = iter.Prev()
	assert.Equal(t, io.EOF, err)

	iter.Close()

	iter, ok = bt.Seek(0xABcd321101)
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Next()
	assert.Equal(t, uint64(0xABcd321200), k) // larger than target
	assert.Equal(t, int64(3), v)
	assert.Equal(t, nil, err)
	k, v, err = iter.Next()
	assert.Equal(t, uint64(0xABd3212000), k)
	assert.Equal(t, int64(4), v)
	assert.Equal(t, nil, err)
	iter.Close()

	iter, ok = bt.Seek(0xABd3212001)
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Prev()
	assert.Equal(t, uint64(0xABd3212000), k)
	assert.Equal(t, int64(4), v)
	assert.Equal(t, nil, err)
	iter.Close()

	iter, ok = bt.Seek(0xDD00000000)
	assert.Equal(t, false, ok) //not exact match
	k, v, err = iter.Prev()
	assert.Equal(t, uint64(0xBBed321400), k) // larger than target
	assert.Equal(t, int64(10), v)
	assert.Equal(t, nil, err)
	iter.Close()

	iter, ok = bt.Seek(0xDD00000000)
	assert.Equal(t, false, ok) //not exact match
	_, v, err = iter.Next()
	assert.Equal(t, int64(0), v)
	assert.Equal(t, io.EOF, err)
	iter.Close()

	bt.Close()
}

