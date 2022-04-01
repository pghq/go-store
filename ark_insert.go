package ark

import "github.com/pghq/go-tea/trail"

// Insert insert a value
func (tx Txn) Insert(table string, v DocumentEncoder) error {
	span := trail.StartSpan(tx, "database.modification")
	defer span.Finish()

	return tx.backend.Insert(span.Context(), table, v.Encode())
}
