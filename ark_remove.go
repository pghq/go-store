package ark

import (
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Remove Delete a value by key
func (tx Txn) Remove(table string, k, v interface{}, opts ...database.CommandOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	return tx.backend.Remove(table, database.NamedKey(v, k), opts...)
}
