#ifndef CPPBTREE_H
#define CPPBTREE_H
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct {
		uint64_t key;
		int64_t value;
		int is_valid;
	} KVPair;

	size_t cppbtree_new();
	void  cppbtree_delete(size_t tree);
	size_t cppbtree_size(size_t tree);

	int64_t cppbtree_put_new_and_get_old(size_t tree, uint64_t key, int64_t value, bool *old_exist);

	void  cppbtree_set(size_t tree, uint64_t key, int64_t value);
	void  cppbtree_erase(size_t tree, uint64_t key);
	int64_t cppbtree_get(size_t tree, uint64_t key, bool* ok);

	size_t cppbtree_seek(size_t tree, uint64_t key, bool* is_equal, bool* larger_than_target, bool* is_valid, bool* ending);

	KVPair iter_prev(size_t iter);
	KVPair iter_next(size_t iter);
	void   iter_delete(size_t iter);

	void cppbtree_set_debug_mode(size_t tree, bool debug);

#ifdef __cplusplus
}
#endif

#endif
