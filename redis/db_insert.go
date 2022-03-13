package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, k, v interface{}, args ...interface{}) error {
	b, err := database.Encode(v)
	if err != nil {
		return tea.Stacktrace(err)
	}

	cmd := tx.unit.Set(tx.ctx, fmt.Sprintf("%s.%s", table, k), b, database.NewRequest(args...).TTL)
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	return nil
}
