package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Get(table, k string, v interface{}, _ ...db.QueryOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read capable tx")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	item, err := tx.reader.Get(tbl.primary.pk([]byte(k)))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NoContent(err)
		}

		return tea.Error(err)
	}

	return item.Value(func(b []byte) error { return db.Decode(b, v) })
}
