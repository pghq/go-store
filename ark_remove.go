package ark

import (
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// Remove Delete a value by key
func (tx Txn) Remove(table string, k interface{}, opts ...db.CommandOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	return tx.backend.Remove(table, k, opts...)
}
