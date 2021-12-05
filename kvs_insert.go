package ark

import (
	"time"

	"github.com/pghq/go-ark/internal"
)

// Insert value
func (tx *KVSTxn) Insert(k []byte, v interface{}) internal.Resolver {
	return tx.update(internal.Insert{Key: k, Value: v})
}

// InsertWithTTL value
func (tx *KVSTxn) InsertWithTTL(k []byte, v interface{}, ttl time.Duration) internal.Resolver {
	return tx.update(internal.Insert{Key: k, Value: v}, ttl)
}
