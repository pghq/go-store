package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Remove(table string, k db.Key, _ ...db.CommandOption) error {
	if tx.reader == nil {
		return tea.NewError("write only")
	}

	doc := tx.Table(table).NewDocument(k)
	if table != "" {
		if doc.AttributeKey != nil {
			var indexes [][]byte
			item, err := tx.reader.Get(doc.AttributeKey)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					err = tea.NotFound(err)
				}
				return tea.Error(err)
			}

			if err := item.Value(func(b []byte) error { return db.Decode(b, &indexes) }); err != nil {
				return tea.Error(err)
			}

			for _, key := range indexes {
				if err := tx.writer.Delete(key); err != nil {
					return tea.Error(err)
				}
			}
		}
	}

	return tx.writer.Delete(doc.PrimaryKey)
}
