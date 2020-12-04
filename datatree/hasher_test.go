package datatree

import (
	"testing"
	sha256 "github.com/minio/sha256-simd"

	"github.com/stretchr/testify/assert"
)

type hashData struct {
	refRes []byte
	impRes []byte
	srcA   []byte
	srcB   []byte
}

func fillHashData(num int, level byte) []hashData {
	res := make([]hashData, num)
	for i := range res {
		res[i] = hashData {
			impRes: make([]byte, 32),
			srcA:   make([]byte, 23),
			srcB:   make([]byte, 40),
		}
		for j := range res[i].srcA {
			res[i].srcA[j] = byte(i)
		}
		for j := range res[i].srcB {
			res[i].srcB[j] = byte(i^(i>>8))
		}
		h := sha256.Sum256(append(append([]byte{level}, res[i].srcA...), res[i].srcB...))
		res[i].refRes = h[:]
	}
	return res
}

func TestHasher(t *testing.T) {
	runHasher(t, 50, 1)
	runHasher(t, 100, 2)
	runHasher(t, 500, 3)
	runHasher(t, 1000, 4)
	runHasher(t, 1700, 5)
	runHasher(t, 2000, 6)
}

func runHasher(t *testing.T, num int, level byte) {
	hasher := Hasher{}
	data := fillHashData(num, level)
	for i := range data {
		hasher.Add(level, data[i].impRes, data[i].srcA, data[i].srcB)
	}
	hasher.Run()
	for i := range data {
		assert.Equal(t, data[i].refRes, data[i].impRes)
	}

	a := []byte("hello")
	b := []byte("world")
	c := append(append([]byte{1}, a...), b...)
	ha := sha256.Sum256(a)
	hb := sha256.Sum256(b)
	hc := sha256.Sum256(c)
	assert.Equal(t, ha[:], hash(a))
	assert.Equal(t, hb[:], hash(b))
	assert.Equal(t, hc[:], hash2(1, a, b))
}

