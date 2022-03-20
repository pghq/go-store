package ark

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Get Retrieve a value
func (tx Txn) Get(table string, k, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	args = append(args, k)
	key := []byte(fmt.Sprintf("%s%s", table, database.NewRequest(args...).CacheKey))
	if cv, present := tx.cache.Get(key); present {
		return database.Copy(cv, v)
	}

	if err := tx.backend.Get(table, k, v, args...); err != nil {
		return tea.Stacktrace(err)
	}

	select {
	case tx.views <- view{Key: key, Value: v}:
	default:
		return nil
	}

	return nil
}
