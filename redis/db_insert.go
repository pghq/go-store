package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, k database.Key, v interface{}, opts ...database.CommandOption) error {
	b, err := database.Encode(v)
	if err != nil {
		return tea.Stack(err)
	}

	cmd := tx.unit.Set(tx.ctx, fmt.Sprintf("%s.%s", table, k), b, database.CommandWith(opts).TTL)
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	return nil
}
