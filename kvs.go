package ark

import (
	"context"

	"github.com/pghq/go-tea"
)

// KVSConn is a mid-level data mapper for KV store instances.
type KVSConn struct {
	mapper *Mapper
}

// Do executes a single transaction
func (c *KVSConn) Do(ctx context.Context, fn func(tx *KVSTxn) error, ro ...bool) error {
	tx, err := c.Txn(ctx, ro...)
	if err != nil {
		return tea.Error(err)
	}

	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return tea.Error(err)
	}

	return tx.Commit()
}
