package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Get(table string, k, v interface{}, _ ...db.QueryOption) error {
	cmd := tx.unit.Get(tx.ctx, fmt.Sprintf("%s.%s", table, k))
	select {
	case tx.reads <- read{cmd: cmd, v: v, limit: 1}:
	default:
		return tea.NewError("read batch size exhausted")
	}

	return nil
}
