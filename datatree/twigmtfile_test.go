package datatree

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func generateTwig(randNum uint32) (twig [4096][32]byte) {
	for i := 2048; i < 4096; i++ {
		for j := 0; j+4 < 32; j += 4 {
			binary.LittleEndian.PutUint32(twig[i][j:j+4], randNum)
			randNum += 257
		}
	}
	syncMTree(twig[:], 0, 2048)
	return
}

func TestTwimMtFile(t *testing.T) {
	_ = os.RemoveAll("./twig")
	_ = os.Mkdir("./twig", 0700)

	tf, err := NewTwigMtFile(64*1024, 1*1024*1024 /*1MB*/, "./twig")
	require.Equal(t, nil, err)

	var twigs [3][4096][32]byte
	twigs[0] = generateTwig(1000)
	twigs[1] = generateTwig(1111111)
	twigs[2] = generateTwig(2222222)

	tf.AppendTwig(twigs[0][1:], 789)
	tf.AppendTwig(twigs[1][1:], 1000789)
	tf.AppendTwig(twigs[2][1:], 2000789)

	tf.Flush()
	tf.Close()

	tf, err = NewTwigMtFile(64*1024, 1*1024*1024 /*1MB*/, "./twig")
	require.Equal(t, nil, err)

	require.Equal(t, int64(789), tf.GetFirstEntryPos(0))
	require.Equal(t, int64(1000789), tf.GetFirstEntryPos(1))
	require.Equal(t, int64(2000789), tf.GetFirstEntryPos(2))

	for twigID := range twigs {
		for i := 1; i < 4096; i++ {
			cache := make(map[int][]byte) // we do not look up this cache
			require.Equal(t, twigs[twigID][i][:], tf.GetHashNode(int64(twigID), i, cache))
		}
	}

	for twigID := range twigs {
		cache := make(map[int][]byte)
		for i := 1; i < 4096; i++ {
			if bz, ok := cache[i]; ok {
				require.Equal(t, twigs[twigID][i][:], bz)
			} else {
				require.Equal(t, twigs[twigID][i][:], tf.GetHashNode(int64(twigID), i, cache))
			}
		}
	}

	tf.Close()

	os.RemoveAll("./twig")
}
