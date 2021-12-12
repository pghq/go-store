package redis

import (
	"fmt"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Insert(table, k string, v interface{}, opts ...db.CommandOption) error {
	b, err := db.Encode(v)
	if err != nil {
		return tea.Error(err)
	}

	cmd := db.CommandWith(opts)
	tx.unit.Set(tx.ctx, fmt.Sprintf("%s.%s", table, k), b, cmd.TTL)
	return nil
}
