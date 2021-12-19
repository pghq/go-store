package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Update(table string, k db.Key, v interface{}, opts ...db.CommandOption) error {
	if tx.reader == nil {
		return tea.NewError("write only")
	}

	doc := tx.Table(table).NewDocument(k)
	if _, err := tx.reader.Get(doc.PrimaryKey); err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NotFound(err)
		}

		return tea.Error(err)
	}

	return tx.Insert(table, k, v, opts...)
}
