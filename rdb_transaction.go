package ark

import (
	"context"

	"github.com/pghq/go-tea"
)

// Txn forks/creates a transaction
func (c *RDBConn) Txn(ctx context.Context, ro ...bool) (*RDBTxn, error) {
	if tx, ok := ctx.(*RDBTxn); ok {
		tx := *tx
		tx.root = false
		return &tx, nil
	}

	mtx, err := c.mapper.txn(ctx, ro...)
	if err != nil {
		return nil, tea.Error(err)
	}

	return &RDBTxn{txn: mtx}, nil
}

// RDBTxn is an RDB transaction
type RDBTxn struct {
	*txn
}
