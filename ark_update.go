package ark

import (
	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-tea/trail"
)

// Update Replace an existing value
func (tx Txn) Update(table string, query database.Query, v DocumentEncoder) error {
	span := trail.StartSpan(tx, "database.modification")
	defer span.Finish()

	return tx.backend.Update(span.Context(), table, query, v.Encode())
}
