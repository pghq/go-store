package ark

import (
	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-tea/trail"
)

// Update Replace an existing value
func (tx Txn) Update(table string, query database.Query, v interface{}) error {
	span := trail.StartSpan(tx, "Database.Update")
	defer span.Finish()

	encoder, ok := v.(DocumentEncoder)
	if !ok {
		encoder = newTransientDocument(v)
	}

	return tx.backend.Update(span.Context(), table, query, encoder.Encode())
}
