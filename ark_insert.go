package ark

import "github.com/pghq/go-tea/trail"

// Insert insert a value
func (tx Txn) Insert(table string, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Database.Insert")
	defer span.Finish()

	encoder, ok := v.(DocumentEncoder)
	if !ok {
		encoder = newTransientDocument(v)
	}

	return tx.backend.Insert(span.Context(), table, encoder.Encode())
}
