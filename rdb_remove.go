package ark

import (
	"github.com/pghq/go-ark/internal"
)

// Remove value
func (tx *RDBTxn) Remove(table string, filter interface{}) internal.Resolver {
	return tx.update(internal.Remove{Table: table, Filter: filter})
}
