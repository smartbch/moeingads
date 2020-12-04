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
	"unsafe"
)

type Enumerator struct {
	tree             C.size_t
	iter             C.size_t
	largerThanTarget bool
}

type Tree struct {
	ptr   C.size_t
}

func TreeNew(_ func(a, b []byte) int) *Tree {
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

func (tree *Tree) PutNewAndGetOld(key []byte, newV uint64) (uint64, bool) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	var oldExists C.int
	oldV := C.cppbtree_put_new_and_get_old(tree.ptr, keydata, C.int(len(key)), C.uint64_t(newV), &oldExists)
	return uint64(oldV), oldExists!=0
}

func (tree *Tree) Set(key []byte, value uint64) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	C.cppbtree_set(tree.ptr, keydata, C.int(len(key)), C.uint64_t(value))
}

func (tree *Tree) Delete(key []byte) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	C.cppbtree_erase(tree.ptr, keydata, C.int(len(key)))
}

func (tree *Tree) Get(key []byte) (uint64, bool) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	var ok C.int
	value := C.cppbtree_get(tree.ptr, keydata, C.int(len(key)), &ok)
	return uint64(value), ok!=0
}

func (tree *Tree) Seek(key []byte) (*Enumerator, bool) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	var is_equal C.int
	e := &Enumerator{
		tree:             tree.ptr,
		iter:             C.cppbtree_seek(tree.ptr, keydata, C.int(len(key)), &is_equal),
		largerThanTarget: is_equal == 0,
	}
	if is_equal == 0 {
		return e, false
	}
	return e, true
}

func (tree *Tree) SeekFirst() (*Enumerator, error) {
	var is_empty C.int
	e := &Enumerator{
		tree: tree.ptr,
		iter: C.cppbtree_seekfirst(tree.ptr, &is_empty),
	}
	if is_empty != 0 {
		return nil, io.EOF
	}
	return e, nil
}

func (e *Enumerator) Close() {
	C.iter_delete(e.iter)
}

func (e *Enumerator) Next() (k []byte, v uint64, err error) {
	if e.tree == 0 { // this Enumerator has been invalidated
		return nil, 0, io.EOF
	}
	e.largerThanTarget = false
	res := C.iter_next(e.tree, e.iter)
	v = uint64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = C.GoBytes(res.key, res.key_len)
	return
}

func (e *Enumerator) Prev() (k []byte, v uint64, err error) {
	if e.tree == 0 { // this Enumerator has been invalidated
		return nil, 0, io.EOF
	}
	var before_begin C.int
	if e.largerThanTarget {
		C.iter_prev(e.tree, e.iter, &before_begin)
		if before_begin != 0 {
			e.tree = 0 // make this Enumerator invalid
			err = io.EOF
			return
		}
	}
	e.largerThanTarget = false
	res := C.iter_prev(e.tree, e.iter, &before_begin)
	v = uint64(res.value)
	err = nil
	k = C.GoBytes(res.key, res.key_len)
	if before_begin != 0 {
		e.tree = 0 // make this Enumerator invalid
	}
	return
}


