#include "./cpp-btree-1.0.1/btree_map.h"
#include "cppbtree.h"
#include <string.h>
#include <stdint.h>
#include <iostream>
using namespace std;

// averagely each 16-byte leaf costs 22 bytes in b-tree

struct mystr {
	union {
		char bytes[8];
		std::string *ptr;
		int64_t i64;
	} payload;
	const char* data() const;
	int size() const;
	void reset();
	mystr(char* key, int key_len);
	mystr();
	mystr(const mystr& other);
	mystr& operator=(const mystr& other);
	mystr(mystr&& other);
	mystr& operator=(mystr&& other);
	~mystr();
	bool operator<(const mystr& other) const;
};

// 00    pointer
// 01/11 8-byte string
// 10    8-byte string

const char* mystr::data() const {
	if((this->payload.bytes[0]&3)!=0) {
		return this->payload.bytes;
	} else {
		return (const char*)(this->payload.ptr->data());
	}
}

void mystr::reset() {
	this->payload.i64=-1;
}

int mystr::size() const {
	if((this->payload.bytes[0]&3)!=0) {
		return 8;
	} else {
		return this->payload.ptr->size();
	}
}

mystr::mystr() {
	this->reset();
}

mystr::mystr(char* key, int key_len) {
	if((key[0]&3)!=0 && key_len==8) {
		//memcpy(this->payload.bytes, key, 8);
		this->payload.i64=*((int64_t*)key);
	} else {
		std::string* ptr = new std::string(key, key_len);
		this->payload.ptr = ptr;
	}
}

mystr::mystr(const mystr& other) {
	const char* key = other.data();
	int key_len = other.size();
	if((key[0]&3)!=0 && key_len==8) {
		//memcpy(this->payload.bytes, key, 8);
		this->payload.i64=*((int64_t*)key);
	} else {
		std::string* ptr = new std::string(key, key_len);
		this->payload.ptr = ptr;
	}
}

mystr& mystr::operator=(const mystr& other) {
	if (&other == this) {
		return *this;
	}
	if((this->payload.bytes[0]&3)==0) {
		delete this->payload.ptr;
	}
	const char* key = other.data();
	int key_len = other.size();
	if((key[0]&3)!=0 && key_len==8) {
		//memcpy(this->payload.bytes, key, 8);
		this->payload.i64=*((int64_t*)key);
	} else {
		std::string* ptr = new std::string(key, key_len);
		this->payload.ptr = ptr;
	}
	return *this;
}

mystr::mystr(mystr&& other) {
	this->payload = other.payload;
	other.reset();
}

mystr& mystr::operator=(mystr&& other) {
	if (&other == this) {
		return *this;
	}
	if((this->payload.bytes[0]&3)==0) {
		delete this->payload.ptr;
	}
	this->payload = other.payload;
	other.reset();
	return *this;
}

mystr::~mystr() {
	if((this->payload.bytes[0]&3)==0) {
		delete this->payload.ptr;
	}
}

bool mystr::operator<(const mystr& other) const {
	if((this->payload.bytes[0]&3)!=0 && (other.payload.bytes[0]&3)!=0) {
		// change little-endian to big-endian
		uint64_t a = __builtin_bswap64(uint64_t(this->payload.i64));
		uint64_t b = __builtin_bswap64(uint64_t(other.payload.i64));
		return a<b;
		//for(int i=0; i<8; i++) {
		//	if(this->payload.bytes[i] < other.payload.bytes[i]) {
		//		return true;
		//	}
		//	if(this->payload.bytes[i] > other.payload.bytes[i]) {
		//		return false;
		//	}
		//}
		//return false;
	}

	std::string a(this->payload.bytes, 8);
	if((this->payload.bytes[0]&3)==0) {
		a = *(this->payload.ptr);
	}
	std::string b(other.payload.bytes, 8);
	if((other.payload.bytes[0]&3)==0) {
		b = *(other.payload.ptr);
	}
	return a<b;
}

typedef btree::btree_map<mystr, uint64_t> BTree;
typedef BTree::iterator Iter;

size_t cppbtree_new() {
	BTree* bt = new BTree();
	return (size_t)bt;
}
void  cppbtree_delete(size_t tree) {
	BTree* bt = (BTree*)tree;
	delete bt;
}

uint64_t cppbtree_size(size_t tree) {
	BTree* bt = (BTree*)tree;
	return bt->size();
}

uint64_t cppbtree_put_new_and_get_old(size_t tree, char* key, int key_len, uint64_t value, int *oldExist) {
	mystr keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr); 
	if (iter == bt->end()) {
		(*bt)[keyStr] = value;
		*oldExist = 0;
		return 0;
	} else {
		uint64_t old_value = iter->second;
		iter->second = value;
		*oldExist = 1;
		return old_value;
	}
}

void  cppbtree_set(size_t tree, char* key, int key_len, uint64_t value) {
	mystr keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	(*bt)[keyStr] = value;
}
void  cppbtree_erase(size_t tree, char* key, int key_len) {
	mystr keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	bt->erase(keyStr);
}
uint64_t cppbtree_get(size_t tree, char* key, int key_len, int* ok) {
	mystr keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr);
	if(iter == bt->end()) {
		*ok = 0;
		return 0;
	} else {
		*ok = 1;
		return iter->second;
	}
}

size_t cppbtree_seek(size_t tree, char* key, int key_len, int* is_equal) {
	Iter* iter = new Iter();
	mystr keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	*iter = bt->lower_bound(keyStr);
	if((*iter) == bt->end()) {
		*is_equal = 0;
		return (size_t)iter;
	}
	if(keyStr < (*iter)->first) {
		*is_equal = 0;
		return (size_t)iter;
	}
	*is_equal = 1;
	return (size_t)iter;
}

size_t cppbtree_seekfirst(size_t tree, int *is_empty) {
	Iter* iter = new Iter();
	BTree* bt = (BTree*)tree;
	if(bt->size() == 0) {
		*is_empty = 1;
		return 0;
	} else {
		*is_empty = 0;
		*iter = bt->begin();
		return (size_t)iter;
	}
}

KVPair iter_next(size_t tree, size_t iter_ptr) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter& iter = *((Iter*)iter_ptr);
	res.is_valid = (bt->end()==iter)? 0 : 1;
	if(res.is_valid == 0) {
		res.key = nullptr;
		res.key_len = 0;
		res.value = 0;
		return res;
	}
	res.key = (void*)iter->first.data();
	res.key_len = iter->first.size();
	res.value = iter->second;
	iter.increment();
	return res;
}
KVPair iter_prev(size_t tree, size_t iter_ptr, int* before_begin) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter& iter = *((Iter*)iter_ptr);
	if(bt->end()==iter) {
		res.key = nullptr;
		res.key_len = 0;
		res.value = 0;
		iter.decrement();
		return res;
	}
	res.key = (void*)iter->first.data();
	res.key_len = iter->first.size();
	res.value = iter->second;
	res.is_valid = 1;
	if(bt->begin()==iter) {
		*before_begin = 1;
	} else {
		*before_begin = 0;
		iter.decrement();
	}
	return res;
}
void  iter_delete(size_t iter_ptr) {
	delete (Iter*)iter_ptr;
}
