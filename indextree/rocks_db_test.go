package indextree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestRocksDB(t *testing.T) {
	os.RemoveAll("./db.db")
	db, err := NewRocksDB("db", "./")
	if err != nil {
		panic(err)
	}
	db.SetSync([]byte{1}, []byte{1})
	value := db.Get([]byte{1})
	assert.Equal(t, []byte{1}, value)
	value = db.Get([]byte{2})
	assert.Equal(t, 0, len(value))

	db.Close()
	db, err = NewRocksDB("db", "./")
	if err != nil {
		panic(err)
	}
	value = db.Get([]byte{1})
	assert.Equal(t, []byte{1}, value)
	value = db.Get([]byte{2})
	assert.Equal(t, 0, len(value))

	os.RemoveAll("./db.db")
}

