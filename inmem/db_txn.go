package inmem

import (
	"context"

	"github.com/dgraph-io/badger/v3"

	"github.com/pghq/go-ark/db"
)

func (d DB) Txn(ctx context.Context, opts ...db.TxnOption) db.Txn {
	config := db.TxnConfigWith(opts)

	tx := txn{
		ctx:   ctx,
		table: d.table,
	}

	if config.BatchWrite {
		batch := d.backend.NewWriteBatch()
		tx.set = batch.SetEntry
		tx.delete = batch.Delete
		tx.commit = batch.Flush
		tx.rollback = batch.Cancel
	} else {
		unit := d.backend.NewTransaction(!config.ReadOnly)
		tx.set = unit.SetEntry
		tx.delete = unit.Delete
		tx.commit = unit.Commit
		tx.rollback = unit.Discard
		tx.reader = unit
	}

	return tx
}

// txn | in-memory database transaction
type txn struct {
	ctx      context.Context
	table    func(name string) (table, error)
	index    func(name string) (index, error)
	reader   *badger.Txn
	set      func(e *badger.Entry) error
	delete   func(key []byte) error
	commit   func() error
	rollback func()
}

func (tx txn) Commit() error {
	return tx.commit()
}

func (tx txn) Rollback() error {
	tx.rollback()
	return nil
}
