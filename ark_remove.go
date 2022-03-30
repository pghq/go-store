package ark

import "github.com/pghq/go-ark/database"

// Remove Delete a value by key
func (tx Txn) Remove(table string, query database.Query) error {
	key := query.Key(table)
	tx.cache.Del(key)
	return tx.backend.Remove(table, query)
}
