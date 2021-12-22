package ark

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// List Retrieve a listing of values
func (tx Txn) List(table string, v interface{}, opts ...database.QueryOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	key := []byte(fmt.Sprintf("%s", database.QueryWith(opts).CacheKey))
	if cv, present := tx.cache.Get(key); present {
		return database.Copy(cv, v)
	}

	if err := tx.backend.List(table, v, opts...); err != nil {
		return tea.Stack(err)
	}

	select {
	case tx.views <- view{Key: key, Value: v}:
	default:
		return tea.Err("read batch size exhausted")
	}

	return nil
}
