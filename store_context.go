package ark

import (
	"context"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/client"
)

// Context creates a new data store context
func (s *Store) Context(ctx context.Context, opts ...Option) (*Context, error) {
	conf := Config{}
	for _, opt := range opts {
		opt.Apply(&conf)
	}

	if ctx != nil {
		if ctx, ok := ctx.(*Context); ok {
			ctx := *ctx
			ctx.child = true
			return &ctx, nil
		}
	}

	return NewContext(ctx, s, conf.ro)
}

// Do a series of subroutines
func (s *Store) Do(ctx context.Context, subroutines ...func(tx *Context) error) error {
	tx, err := s.Context(ctx)
	if err != nil {
		return tea.Error(err)
	}

	for _, f := range subroutines {
		if err := f(tx); err != nil {
			_ = tx.Rollback()
			return tea.Error(err)
		}
	}

	return tx.Commit()
}

// Context is a data store transactions
type Context struct {
	child bool
	context.Context
	store *Store
	tx    client.Transaction
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
func NewContext(ctx context.Context, store *Store, ro bool) (*Context, error) {
	tx, err := store.client.Transaction(ctx, ro)
	if err != nil {
		return nil, tea.Error(err)
	}

	c := Context{
		Context: ctx,
		store:   store,
		tx:      tx,
	}

	return &c, nil
}
