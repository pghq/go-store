package sql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (d DB) Txn(ctx context.Context, opts ...database.TxnOption) database.Txn {
	if d.err != nil {
		return txn{err: d.err}
	}

	config := database.TxnConfigWith(opts)
	tx, err := d.backend.BeginTxx(ctx, &sql.TxOptions{ReadOnly: config.ReadOnly && !config.BatchWrite})
	return txn{
		ctx:  ctx,
		unit: tx,
		err:  err,
		ph:   d.ph,
	}
}

// txn SQL transaction
type txn struct {
	ctx  context.Context
	ph   placeholder
	unit *sqlx.Tx
	err  error
}

func (tx txn) Commit() error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	return tx.unit.Commit()
}

func (tx txn) Rollback() error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	return tx.unit.Rollback()
}
