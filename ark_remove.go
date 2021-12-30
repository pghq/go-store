package ark

import (
	"github.com/pghq/go-tea"
)

// Remove Delete a value by key
func (tx Txn) Remove(table string, k interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	return tx.backend.Remove(table, k, args...)
}
