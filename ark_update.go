package ark

import (
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Update Replace an existing value
func (tx Txn) Update(table string, k, v interface{}, opts ...database.CommandOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	return tx.backend.Update(table, database.NamedKey(v, k), v, opts...)
}
