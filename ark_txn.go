package ark

import (
	"context"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// Txn | Begin or fork a transaction
func (m *Mapper) Txn(ctx context.Context, opts ...db.TxnOption) Txn {
	if tx, ok := ctx.(Txn); ok {
		tx.root = false
		return tx
	}

	config := db.TxnConfigWith(opts)
	return Txn{
		Context: ctx,
		cache:   m.cache,
		backend: m.db.Txn(ctx, opts...),
		root:    true,
		opts:    opts,
		err:     m.err,
		views:   make(chan view, config.BatchReadSize),
	}
}

// Do | Write and or read using a callback
func (m *Mapper) Do(ctx context.Context, fn func(tx db.Txn) error, opts ...db.TxnOption) error {
	if m.err != nil {
		return tea.Error(m.err)
	}

	tx := m.Txn(ctx, opts...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Error(err)
	}

	return tx.Commit()
}

// View | Read using a callback
func (m *Mapper) View(ctx context.Context, fn func(tx db.Txn) error, opts ...db.TxnOption) error {
	if m.err != nil {
		return tea.Error(m.err)
	}

	tx := m.Txn(ctx, append(opts, db.ReadOnly())...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Error(err)
	}

	return tx.Commit()
}

// Txn | A unit of work
type Txn struct {
	context.Context
	root    bool
	backend db.Txn
	cache   *ristretto.Cache
	views   chan view
	opts    []db.TxnOption
	err     error
}

// Commit | Submit a unit of work
func (tx Txn) Commit() error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	if !tx.root {
		return nil
	}

	if err := tx.backend.Commit(); err != nil {
		return tea.Error(err)
	}

	config := db.TxnConfigWith(tx.opts)
	for {
		select {
		case vw := <-tx.views:
			if config.ViewTTL != 0 {
				tx.cache.SetWithTTL(vw.Key, vw.Value, 1, config.ViewTTL)
			}
		default:
			return nil
		}
	}
}

// Rollback | Cancel a unit of work
func (tx Txn) Rollback() error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	if !tx.root {
		return nil
	}

	return tx.backend.Rollback()
}

// view | A single view to be cached
type view struct {
	Key   []byte
	Value interface{}
}
