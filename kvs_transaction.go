package ark

import (
	"context"

	"github.com/pghq/go-tea"
)

// Txn forks/creates a transaction
func (c *KVSConn) Txn(ctx context.Context, ro ...bool) (*KVSTxn, error) {
	if tx, ok := ctx.(*KVSTxn); ok {
		tx := *tx
		tx.root = false
		return &tx, nil
	}

	mtx, err := c.mapper.txn(ctx, ro...)
	if err != nil {
		return nil, tea.Error(err)
	}

	return &KVSTxn{txn: mtx}, nil
}

// KVSTxn is a KVS transaction
type KVSTxn struct {
	*txn
}
