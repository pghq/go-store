package ark

import (
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// Insert Create a value with a key
func (tx Txn) Insert(table string, k, v interface{}, opts ...db.CommandOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	return tx.backend.Insert(table, db.NamedKey(v, k), v, opts...)
}
