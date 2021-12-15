package ark

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// Get Retrieve a value
func (tx Txn) Get(table string, k, v interface{}, opts ...db.QueryOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	key := []byte(fmt.Sprintf("%s%s", table, k))
	if cv, present := tx.cache.Get(key); present {
		return db.Copy(cv, v)
	}

	if err := tx.backend.Get(table, k, v, opts...); err != nil {
		return tea.Error(err)
	}

	select {
	case tx.views <- view{Key: key, Value: v}:
	default:
		return tea.NewError("read batch size exhausted")
	}

	return nil
}
