package redis

import (
	"fmt"

	"github.com/pghq/go-tea"
)

func (tx txn) Get(table string, k, v interface{}, _ ...interface{}) error {
	cmd := tx.unit.Get(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	select {
	case tx.reads <- read{cmd: cmd, v: v, limit: 1}:
	default:
		return tea.Err("read batch size exhausted")
	}

	return nil
}
