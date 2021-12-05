package ark

import "github.com/pghq/go-ark/internal"

// List values
func (tx *RDBTxn) List(query Query, dst interface{}, consistent ...bool) internal.Resolver {
	return tx.view(query.l, dst, consistent...)
}
