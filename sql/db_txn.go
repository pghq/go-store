package sql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (d DB) Txn(ctx context.Context, opts ...db.TxnOption) db.Txn {
	if d.err != nil {
		return txn{err: d.err}
	}

	config := db.TxnConfigWith(opts)
	tx, err := d.backend.BeginTxx(ctx, &sql.TxOptions{ReadOnly: config.ReadOnly && !config.BatchWrite})
	return txn{
		ctx:  ctx,
		unit: tx,
		err:  err,
	}
}

// txn SQL transaction
type txn struct {
	ctx  context.Context
	unit *sqlx.Tx
	err  error
}

func (tx txn) Commit() error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	return tx.unit.Commit()
}

func (tx txn) Rollback() error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	return tx.unit.Rollback()
}
