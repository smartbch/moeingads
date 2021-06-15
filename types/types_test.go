package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetShardID(t *testing.T) {
	for i := byte(0); i <= 64; i++ {
		assert.Equal(t, 0, GetShardID(i))
	}
	for i := 0; i < 8; i++ {
		assert.Equal(t, i, GetShardID(byte(64+i*16)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+1)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+2)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+3)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+4)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+5)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+6)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+7)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+8)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+9)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+10)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+11)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+12)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+13)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+14)))
		assert.Equal(t, i, GetShardID(byte(64+i*16+15)))
	}
	assert.Equal(t, 7, GetShardID(64+128))
	assert.Equal(t, 7, GetShardID(64+129))
	assert.Equal(t, 7, GetShardID(254))
	assert.Equal(t, 7, GetShardID(255))
}

