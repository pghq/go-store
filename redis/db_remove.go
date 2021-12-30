package redis

import (
	"fmt"

	"github.com/pghq/go-tea"
)

func (tx txn) Remove(table string, k interface{}, _ ...interface{}) error {
	cmd := tx.unit.Del(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	return nil
}
