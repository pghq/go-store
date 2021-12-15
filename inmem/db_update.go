package inmem

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Update(table string, k, v interface{}, opts ...db.CommandOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read capable tx")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	key := []byte(fmt.Sprintf("%s", k))
	if _, err := tx.reader.Get(tbl.primary.pk(key)); err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NotFound(err)
		}

		return tea.Error(err)
	}

	return tx.Insert(table, k, v, opts...)
}
