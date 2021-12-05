package ark

import (
	"github.com/pghq/go-ark/internal"
)

// Remove value
func (tx *KVSTxn) Remove(k []byte) internal.Resolver {
	return tx.update(internal.Remove{Key: k})
}
