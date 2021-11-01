package datastore

import (
	"context"

	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Context creates a new data store context
func (r *Repository) Context(ctx context.Context) (*Context, func() error, error) {
	if ctx, ok := ctx.(*Context); ok{
		return ctx, func() error{ return nil }, nil
	}

	tx, err := NewContext(ctx, r)
	if err != nil{
		return nil, nil, errors.Wrap(err)
	}

	return tx, tx.tx.Rollback, nil
}

// Context is a data store transactions
type Context struct {
	context.Context
	repo *Repository
	tx   client.Transaction
}

// Commit a datastore transaction
func (ctx *Context) Commit() error {
	return ctx.tx.Commit()
}

// NewContext creates a new instance of the data store context
func NewContext(ctx context.Context, repo *Repository) (*Context, error) {
	tx, err := repo.client.Transaction(ctx)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	c := Context{
		Context: ctx,
		repo: repo,
		tx:   tx,
	}

	return &c, nil
}
