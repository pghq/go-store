package ark

import (
	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-tea/trail"
)

// Remove Delete a value by key
func (tx Txn) Remove(table string, query database.Query) error {
	span := trail.StartSpan(tx, "database.modification")
	defer span.Finish()

	key := query.Key(table)
	tx.cache.Del(key)
	return tx.backend.Remove(span.Context(), table, query)
}
