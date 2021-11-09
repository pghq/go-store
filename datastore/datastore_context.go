package datastore

import (
	"context"

	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Context creates a new data store context
func (r *Repository) Context(ctx context.Context) (*Context, error) {
	if ctx != nil {
		if ctx, ok := ctx.(*Context); ok {
			ctx := *ctx
			ctx.child = true
			return &ctx, nil
		}
	}

	return NewContext(ctx, r)
}

// Procedure executes a series of subroutines and bails fast if any errors occur
func (r *Repository) Procedure(ctx context.Context, subroutines ...func(tx *Context) error) error {
	tx, err := r.Context(ctx)
	if err != nil {
		return errors.Wrap(err)
	}

	for _, f := range subroutines {
		if err := f(tx); err != nil {
			_ = tx.Rollback()
			return errors.Wrap(err)
		}
	}

	return tx.Commit()
}

// Context is a data store transactions
type Context struct {
	child bool
	context.Context
	repo *Repository
	tx   client.Transaction
}

// Commit a datastore transaction
func (ctx *Context) Commit() error {
	if ctx.child {
		return nil
	}

	return ctx.tx.Commit()
}

// Rollback a datastore transaction
func (ctx *Context) Rollback() error {
	if ctx.child {
		return nil
	}

	return ctx.tx.Rollback()
}

// NewContext creates a new instance of the data store context
func NewContext(ctx context.Context, repo *Repository) (*Context, error) {
	tx, err := repo.client.Transaction(ctx)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	c := Context{
		Context: ctx,
		repo:    repo,
		tx:      tx,
	}

	return &c, nil
}
