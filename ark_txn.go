package ark

import (
	"context"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// Txn Begin or fork a transaction
func (m *Mapper) Txn(ctx context.Context, opts ...database.TxnOption) Txn {
	if tx, ok := ctx.(Txn); ok {
		tx.root = false
		return tx
	}

	config := database.TxnConfigWith(opts)
	return Txn{
		Context: ctx,
		cache:   m.cache,
		backend: m.db.Txn(ctx, opts...),
		root:    true,
		config:  config,
	}
}

// Do Write and or read using a callback
func (m *Mapper) Do(ctx context.Context, fn func(tx Txn) error, opts ...database.TxnOption) error {
	if err := ctx.Err(); err != nil {
		return tea.Stacktrace(err)
	}

	tx := m.Txn(ctx, opts...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Stacktrace(err)
	}

	return tx.Commit()
}

// View Read using a callback
func (m *Mapper) View(ctx context.Context, fn func(tx Txn) error, opts ...database.TxnOption) error {
	if err := ctx.Err(); err != nil {
		return tea.Stacktrace(err)
	}

	tx := m.Txn(ctx, append(opts, database.ReadOnly())...)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return tea.Stacktrace(err)
	}

	return tx.Commit()
}

// Txn A unit of work
type Txn struct {
	context.Context
	root    bool
	backend database.Txn
	cache   *ristretto.Cache
	config  database.TxnConfig
}

// Commit Submit a unit of work
func (tx Txn) Commit() error {
	if !tx.root {
		return nil
	}

	return tx.backend.Commit()
}

// Rollback Cancel a unit of work
func (tx Txn) Rollback() error {
	if !tx.root {
		return nil
	}

	return tx.backend.Rollback()
}
