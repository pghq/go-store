package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Get(table string, k db.Key, v interface{}, _ ...db.QueryOption) error {
	if tx.reader == nil {
		return tea.NewError("write only")
	}

	doc := tx.Table(table).NewDocument(k)
	item, err := tx.reader.Get(doc.PrimaryKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.NotFound(err)
		}

		return tea.Error(err)
	}

	if err := item.Value(func(b []byte) error { return doc.Decode(b) }); err != nil {
		return tea.Error(err)
	}

	return doc.Copy(v)
}
