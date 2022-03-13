package ark

import (
	"github.com/pghq/go-tea"
)

// Update Replace an existing value
func (tx Txn) Update(table string, k, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	return tx.backend.Update(table, k, v, args...)
}
