package inmem

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Get(table string, k, v interface{}, _ ...db.QueryOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read capable tx")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	key := []byte(fmt.Sprintf("%s", k))
	item, err := tx.reader.Get(tbl.primary.pk(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NotFound(err)
		}

		return tea.Error(err)
	}

	return item.Value(func(b []byte) error { return db.Decode(b, v) })
}
