package ark

import (
	"github.com/pghq/go-ark/database"
)

// Update Replace an existing value
func (tx Txn) Update(table string, query database.Query, v interface{}) error {
	return tx.backend.Update(table, query, v)
}
