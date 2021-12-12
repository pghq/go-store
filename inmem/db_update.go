package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Update(table, k string, v interface{}, opts ...db.CommandOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read capable tx")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	if _, err := tx.reader.Get(tbl.primary.pk([]byte(k))); err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NoContent(err)
		}

		return tea.Error(err)
	}

	return tx.Insert(table, k, v, opts...)
}
