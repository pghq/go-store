package ark

import (
	"github.com/pghq/go-ark/internal"
)

// Get value
func (tx *KVSTxn) Get(k []byte, v interface{}, consistent ...bool) internal.Resolver {
	return tx.view(internal.Get{Key: k}, v, consistent...)
}
