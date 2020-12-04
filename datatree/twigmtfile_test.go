package datatree

import (
	"os"
	"testing"
	"encoding/binary"

	"github.com/stretchr/testify/assert"
)

func generateTwig(start uint32) [][32]byte{
	res := make([][32]byte, TwigMtEntryCount)
	for i := range res {
		for j := 0; j+4 < 32; j+=4 {
			binary.LittleEndian.PutUint32(res[i][j:j+4], start)
			start += 257
		}
	}
	return res
}

func TestTwimMtFile(t *testing.T) {
	os.RemoveAll("./twig")
	os.Mkdir("./twig", 0700)

	tf, err := NewTwigMtFile(64*1024, 1*1024*1024/*1MB*/, "./twig")
	assert.Equal(t, nil, err)

	twig0 := generateTwig(1000)
	twig1 := generateTwig(1111111)
	twig2 := generateTwig(2222222)

	tf.AppendTwig(twig0, 789)
	tf.AppendTwig(twig1, 1000789)
	tf.AppendTwig(twig2, 2000789)

	tf.Flush()
	tf.Close()

	tf, err = NewTwigMtFile(64*1024, 1*1024*1024/*1MB*/, "./twig")
	assert.Equal(t, nil, err)

	assert.Equal(t, int64(789), tf.GetFirstEntryPos(0))
	assert.Equal(t, int64(1000789), tf.GetFirstEntryPos(1))
	assert.Equal(t, int64(2000789), tf.GetFirstEntryPos(2))

	for i := 0; i<TwigMtEntryCount; i++ {
		assert.Equal(t, twig0[i][:], tf.GetHashNode(0, i+1))
		assert.Equal(t, twig1[i][:], tf.GetHashNode(1, i+1))
		assert.Equal(t, twig2[i][:], tf.GetHashNode(2, i+1))
	}

	tf.Close()

	os.RemoveAll("./twig")
}
