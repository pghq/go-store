package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Update(table string, k database.Key, v interface{}, opts ...database.CommandOption) error {
	cmd := tx.backend.Exists(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	if cmd.Val() == 0 {
		return tea.ErrNotFound("key not found")
	}

	return tx.Insert(table, k, v, opts...)
}
