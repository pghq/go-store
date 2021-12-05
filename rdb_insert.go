package ark

import (
	"github.com/pghq/go-ark/internal"
)

// Insert value
func (tx *RDBTxn) Insert(table string, value interface{}) internal.Resolver {
	return tx.update(internal.Insert{Table: table, Value: value})
}
