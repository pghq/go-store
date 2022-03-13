package ark

import (
	"time"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Insert insert a value
func (tx Txn) Insert(table string, k, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	return tx.backend.Insert(table, k, v, args...)
}

// InsertTTL insert a value with a ttl
func (tx Txn) InsertTTL(table string, k, v interface{}, expire time.Duration) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	return tx.backend.Insert(table, k, v, database.Expire(expire))
}
