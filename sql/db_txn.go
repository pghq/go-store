package sql

import (
	"context"
	"database/sql"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (d DB) Txn(ctx context.Context, opts ...database.TxnOption) database.Txn {
	if d.err != nil {
		return txn{err: d.err}
	}

	config := database.TxnConfigWith(opts)
	uow, err := d.backend.Txn(ctx, &sql.TxOptions{ReadOnly: config.ReadOnly && !config.BatchWrite})
	return txn{
		ctx: ctx,
		uow: uow,
		err: err,
		ph:  d.backend.placeholder(),
	}
}

// txn SQL transaction
type txn struct {
	ctx context.Context
	ph  placeholder
	uow uow
	err error
}

func (tx txn) Commit() error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	return tx.uow.Commit(tx.ctx)
}

func (tx txn) Rollback() error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	return tx.uow.Rollback(tx.ctx)
}

type uow interface {
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	List(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Exec(ctx context.Context, query string, args ...interface{}) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
