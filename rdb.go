package ark

import (
	"context"

	"github.com/pghq/go-tea"
)

// RDBConn is a mid-level data mapper for relational db instances
type RDBConn struct {
	mapper *Mapper
}

// Do executes a single transaction
func (c *RDBConn) Do(ctx context.Context, fn func(tx *RDBTxn) error, ro ...bool) error {
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
