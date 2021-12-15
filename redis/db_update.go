package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Update(table string, k, v interface{}, opts ...db.CommandOption) error {
	if tx.backend.Exists(tx.ctx, fmt.Sprintf("%s.%s", table, k)).Val() == 0 {
		return tea.NewNotFound("key not found")
	}

	return tx.Insert(table, k, v, opts...)
}
