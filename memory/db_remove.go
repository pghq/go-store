package memory

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Remove(table string, k database.Key, _ ...database.CommandOption) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "remove")
	span.Tag("key", k.String())
	doc := tx.Table(table).NewDocument(k)
	if table != "" {
		if doc.AttributeKey != nil {
			var indexes [][]byte
			item, err := tx.reader.Get(doc.AttributeKey)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					err = tea.AsErrNotFound(err)
				}
				return tea.Stack(err)
			}

			if err := item.Value(func(b []byte) error { return database.Decode(b, &indexes) }); err != nil {
				return tea.Stack(err)
			}

			for _, key := range indexes {
				if err := tx.writer.Delete(key); err != nil {
					return tea.Stack(err)
				}
			}
		}
	}

	return tx.writer.Delete(doc.PrimaryKey)
}
