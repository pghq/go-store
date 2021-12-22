package ark

import (
	"context"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Txn Begin or fork a transaction
func (m *Mapper) Txn(ctx context.Context, opts ...database.TxnOption) Txn {
	if err := m.Error(); err != nil {
		return Txn{err: err}
	}

	if tx, ok := ctx.(Txn); ok {
		tx.root = false
		return tx
	}

	span := tea.Nest(ctx, "transaction")
	config := database.TxnConfigWith(opts)
	return Txn{
		Context: span,
		span:    span,
		cache:   m.cache,
		backend: m.db.Txn(span, opts...),
		root:    true,
		opts:    opts,
		views:   make(chan view, config.BatchReadSize),
	}
}

// Do Write and or read using a callback
func (m *Mapper) Do(ctx context.Context, fn func(tx Txn) error, opts ...database.TxnOption) error {
	if err := ctx.Err(); err != nil {
		return tea.Stack(err)
	}

	tx := m.Txn(ctx, opts...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Stack(err)
	}

	return tx.Commit()
}

// View Read using a callback
func (m *Mapper) View(ctx context.Context, fn func(tx Txn) error, opts ...database.TxnOption) error {
	if err := ctx.Err(); err != nil {
		return tea.Stack(err)
	}

	tx := m.Txn(ctx, append(opts, database.ReadOnly())...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Stack(err)
	}

	return tx.Commit()
}

// Txn A unit of work
type Txn struct {
	context.Context
	span    tea.Span
	root    bool
	backend database.Txn
	cache   *ristretto.Cache
	views   chan view
	opts    []database.TxnOption
	err     error
}

// Commit Submit a unit of work
func (tx Txn) Commit() error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	if !tx.root {
		return nil
	}

	defer tx.span.End()
	if err := tx.backend.Commit(); err != nil {
		return tea.Stack(err)
	}

	config := database.TxnConfigWith(tx.opts)
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

// Rollback Cancel a unit of work
func (tx Txn) Rollback() error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	if !tx.root {
		return nil
	}

	defer tx.span.End()
	return tx.backend.Rollback()
}

// view A single view to be cached
type view struct {
	Key   []byte
	Value interface{}
}
