package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Remove(table string, k database.Key, _ ...database.CommandOption) error {
	cmd := tx.unit.Del(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	return nil
}
