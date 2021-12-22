package ark

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Get Retrieve a value
func (tx Txn) Get(table string, k, v interface{}, opts ...database.QueryOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	key := []byte(fmt.Sprintf("%s%s", table, k))
	if cv, present := tx.cache.Get(key); present {
		return database.Copy(cv, v)
	}

	if err := tx.backend.Get(table, database.NamedKey(v, k), v, opts...); err != nil {
		return tea.Stack(err)
	}

	select {
	case tx.views <- view{Key: key, Value: v}:
	default:
		return tea.Err("read batch size exhausted")
	}

	return nil
}
