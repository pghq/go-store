package memory

import (
	"context"

	"github.com/dgraph-io/badger/v3"

	"github.com/pghq/go-ark/database"
)

func (d DB) Txn(ctx context.Context, opts ...database.TxnOption) database.Txn {
	config := database.TxnConfigWith(opts)

	tx := txn{
		DB:  d,
		ctx: ctx,
	}

	if config.BatchWrite {
		backend := d.backend.NewWriteBatch()
		tx.writer = backend
		tx.commit = backend.Flush
		tx.rollback = backend.Cancel
	} else {
		backend := d.backend.NewTransaction(!config.ReadOnly)
		tx.writer = backend
		tx.reader = backend
		tx.commit = backend.Commit
		tx.rollback = backend.Discard
	}

	return tx
}

// txn in-memory database transaction
type txn struct {
	DB
	ctx    context.Context
	reader interface {
		Get(key []byte) (*badger.Item, error)
		NewIterator(opt badger.IteratorOptions) *badger.Iterator
	}
	writer interface {
		SetEntry(e *badger.Entry) error
		Delete(k []byte) error
	}
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
