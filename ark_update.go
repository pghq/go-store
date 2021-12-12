package ark

import (
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// Update | Replace an existing value
func (tx Txn) Update(table, k string, v interface{}, opts ...db.CommandOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	return tx.backend.Update(table, k, v, opts...)
}
