#include <string.h>
#include <stdint.h>
#include <iostream>
#include "bigmap.h"
#include "cppbtree.h"
using namespace std;

typedef bigmap<(1<<16), bits48, bits48> BTree;
typedef BTree::iterator Iter;

size_t cppbtree_new() {
	BTree* bt = new BTree();
	return (size_t)bt;
}
void  cppbtree_delete(size_t tree) {
	BTree* bt = (BTree*)tree;
	delete bt;
}

size_t cppbtree_size(size_t tree) {
	BTree* bt = (BTree*)tree;
	return bt->size();
}

int64_t cppbtree_put_new_and_get_old(size_t tree, uint64_t key, int64_t value, bool *old_exist) {
	BTree* bt = (BTree*)tree;
	auto k = bits48::from_uint64(key);
	auto v = bits48::from_int64(value);
	return bt->put_new_and_get_old(key>>48, k, v, old_exist).to_int64();
}
void  cppbtree_set(size_t tree, uint64_t key, int64_t value) {
	BTree* bt = (BTree*)tree;
	auto k = bits48::from_uint64(key);
	auto v = bits48::from_int64(value);
	bt->set(key>>48, k, v);
}
void  cppbtree_erase(size_t tree, uint64_t key) {
	BTree* bt = (BTree*)tree;
	auto k = bits48::from_uint64(key);
	bt->erase(key>>48, k);
}
int64_t cppbtree_get(size_t tree, uint64_t key, bool* ok) {
	BTree* bt = (BTree*)tree;
	auto k = bits48::from_uint64(key);
	auto v = bt->get(key>>48, k, ok);
	if(*ok) {
		int64_t old_value = v.to_int64();
		return old_value;
	} else {
		return 0;
	}
}

size_t cppbtree_seek(size_t tree, uint64_t key, bool* is_equal, bool* larger_than_target, bool* is_valid, bool* ending) {
	BTree* bt = (BTree*)tree;
	auto k = bits48::from_uint64(key);
	Iter* iter = new Iter();
	*iter = bt->get_iterator(key>>48, k);
	*is_equal = iter->valid() && (iter->key().to_uint64()<<16) == (key<<16);
	*larger_than_target = !(*is_equal);
	*ending = false;
	if(!iter->valid()) { // try to get a valid iterator
		*iter = bt->get_ending_iterator();
		*larger_than_target = false;
		*ending = true;
	}
	*is_valid = iter->valid();
	return (size_t)iter;
}

KVPair iter_move(size_t iter_ptr, bool to_next) {
	KVPair res;
	Iter& iter = *((Iter*)iter_ptr);
	res.is_valid = iter.valid();
	if(res.is_valid == 0) {
		res.key = ~uint64_t(0);
		res.value = -1;
		return res;
	}
	res.key = (uint64_t(iter.curr_idx())<<48)|iter.key().to_uint64();
	res.value = iter.value().to_int64();
	if(to_next) {
		iter.next();
	} else {
		iter.prev();
	}
	return res;
}
KVPair iter_next(size_t iter_ptr) {
	return iter_move(iter_ptr, true);
}
KVPair iter_prev(size_t iter_ptr) {
	return iter_move(iter_ptr, false);
}
void  iter_delete(size_t iter_ptr) {
	delete (Iter*)iter_ptr;
}
void cppbtree_set_debug_mode(size_t tree, bool debug) {
	BTree* bt = (BTree*)tree;
	bt->set_debug_mode(debug);
}
