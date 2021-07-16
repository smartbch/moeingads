package fuzz

type RefL1 struct {
	cache map[string]string
	base  map[string]string
}

func NewRefL1() *RefL1 {
	return &RefL1{
		cache: make(map[string]string),
		base:  make(map[string]string),
	}
}

func (r *RefL1) Get(key []byte) []byte {
	if k, ok := r.cache[string(key)]; ok {
		return []byte(k)
	}
	if k, ok := r.base[string(key)]; ok {
		return []byte(k)
	}
	return nil
}

func (r *RefL1) Set(key, value []byte) {
	r.cache[string(key)] = string(value)
}

func (r *RefL1) Delete(key []byte) {
	r.cache[string(key)] = ""
}

func (r *RefL1) Size() int {
	return len(r.base)
}

func (r *RefL1) FlushCache() {
	for k, v := range r.cache {
		if len(v) == 0 {
			delete(r.base, k)
		} else {
			r.base[k] = v
		}
	}
	r.cache = make(map[string]string)
}

func (r *RefL1) ClearCache() {
	r.cache = make(map[string]string)
}

type RefL2 struct {
	cache map[string]string
	refL1 *RefL1
}

func NewRefL2(r *RefL1) *RefL2 {
	return &RefL2{
		cache: make(map[string]string),
		refL1: r,
	}
}

func (r *RefL2) Get(key []byte) []byte {
	if k, ok := r.cache[string(key)]; ok {
		return []byte(k)
	}
	return r.refL1.Get(key)
}

func (r *RefL2) Set(key, value []byte) {
	r.cache[string(key)] = string(value)
}

func (r *RefL2) Delete(key []byte) {
	r.cache[string(key)] = ""
}

func (r *RefL2) Close(writeBack bool) {
	if writeBack {
		for k, v := range r.cache {
			if len(v) == 0 {
				r.refL1.Delete([]byte(k))
			} else {
				r.refL1.Set([]byte(k), []byte(v))
			}
		}
	}
	r.cache, r.refL1 = nil, nil
}
