package ark

import (
	"github.com/pghq/go-ark/internal"
)

// Update value
func (tx *RDBTxn) Update(table string, filter, value interface{}) internal.Resolver {
	return tx.update(internal.Update{Table: table, Filter: filter, Value: value})
}
