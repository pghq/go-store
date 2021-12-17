package inmem

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Remove(table string, k interface{}, _ ...db.CommandOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read tx")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	key := []byte(fmt.Sprintf("%s", k))
	if table != "" {
		ck := tbl.primary.ck(key)
		var composite [][]byte
		item, err := tx.reader.Get(ck)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				err = tea.NotFound(err)
			}
			return tea.Error(err)
		}

		if err := item.Value(func(b []byte) error { return db.Decode(b, &composite) }); err != nil {
			return tea.Error(err)
		}

		for _, key := range composite {
			if err := tx.delete(key); err != nil {
				return tea.Error(err)
			}
		}
	}

	return tx.delete(tbl.primary.pk(key))
}
