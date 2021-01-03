// +build cppbtree

package cppbtree

/*
#cgo CXXFLAGS: -O3 -std=c++11 
#cgo LDFLAGS: -lstdc++
#include "cppbtree.h"
*/
import "C"

import (
	"io"
)

type Enumerator struct {
	iter             C.size_t
	largerThanTarget bool
}

type Tree struct {
	ptr   C.size_t
}

func TreeNew() *Tree {
	return &Tree{
		ptr: C.cppbtree_new(),
	}
}

func (tree *Tree) Len() int {
	return int(C.cppbtree_size(tree.ptr))
}

func (tree *Tree) Close() {
	C.cppbtree_delete(tree.ptr);
}

func (tree *Tree) PutNewAndGetOld(key uint64, newV int64) (int64, bool) {
	var oldExists C.bool
	oldV := C.cppbtree_put_new_and_get_old(tree.ptr, C.uint64_t(key), C.int64_t(newV), &oldExists)
	return int64(oldV), bool(oldExists)
}

func (tree *Tree) Set(key uint64, value int64) {
	C.cppbtree_set(tree.ptr, C.uint64_t(key), C.int64_t(value))
}

func (tree *Tree) Delete(key uint64) {
	C.cppbtree_erase(tree.ptr, C.uint64_t(key))
}

func (tree *Tree) Get(key uint64) (int64, bool) {
	var ok C.bool
	value := C.cppbtree_get(tree.ptr, C.uint64_t(key), &ok)
	return int64(value), bool(ok)
}

func (tree *Tree) Seek(key uint64) (*Enumerator, bool) {
	var is_equal C.bool
	e := &Enumerator{
		iter:             C.cppbtree_seek(tree.ptr, C.uint64_t(key), &is_equal),
		largerThanTarget: bool(is_equal),
	}
	if !is_equal {
		return e, false
	}
	return e, true
}

func (tree *Tree) SeekFirst() (*Enumerator, bool) {
	return tree.Seek(0)
}

func (e *Enumerator) Close() {
	C.iter_delete(e.iter)
}

func (e *Enumerator) Next() (k uint64, v int64, err error) {
	e.largerThanTarget = false
	res := C.iter_next(e.iter)
	v = int64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = uint64(res.key)
	return
}

func (e *Enumerator) Prev() (k uint64, v int64, err error) {
	if e.largerThanTarget {
		C.iter_prev(e.iter)
	}
	e.largerThanTarget = false
	res := C.iter_prev(e.iter)
	v = int64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = uint64(res.key)
	return
}


