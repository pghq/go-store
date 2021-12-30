package redis

import (
	"fmt"

	"github.com/pghq/go-tea"
)

func (tx txn) Update(table string, k, v interface{}, args ...interface{}) error {
	cmd := tx.backend.Exists(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	if cmd.Val() == 0 {
		return tea.ErrNotFound("key not found")
	}

	return tx.Insert(table, k, v, args...)
}
