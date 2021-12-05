package ark

import "github.com/pghq/go-ark/internal"

// Get value
func (tx *RDBTxn) Get(query Query, dst interface{}, consistent ...bool) internal.Resolver {
	return tx.view(query.get(), dst, consistent...)
}
