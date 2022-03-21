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
	ending           bool
}

type Tree struct {
	ptr C.size_t
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
	C.cppbtree_delete(tree.ptr)
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
	e, equal, _ := tree._seek(key)
	return e, equal
}
func (tree *Tree) _seek(key uint64) (e *Enumerator, isEqual, isValid bool) {
	var is_equal C.bool
	var larger_than_target C.bool
	var is_valid C.bool
	var ending C.bool
	e = &Enumerator{}
	e.iter = C.cppbtree_seek(tree.ptr, C.uint64_t(key), &is_equal, &larger_than_target, &is_valid, &ending)
	e.largerThanTarget = bool(larger_than_target)
	e.ending = bool(ending)
	return e, bool(is_equal), bool(is_valid)
}

func (tree *Tree) SeekFirst() (*Enumerator, error) {
	e, _, valid := tree._seek(0)
	if valid {
		e.largerThanTarget = false
		return e, nil
	}
	return e, io.EOF
}

func (e *Enumerator) Close() {
	C.iter_delete(e.iter)
}

func (e *Enumerator) Next() (k uint64, v int64, err error) {
	if e.ending {
		return 0, 0, io.EOF
	}
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
	e.ending = false
	res := C.iter_prev(e.iter)
	v = int64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = uint64(res.key)
	return
}

func (tree *Tree) SetDebug(debug bool) {
	C.cppbtree_set_debug_mode(tree.ptr, C.bool(debug))
}
