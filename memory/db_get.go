package memory

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Get(table string, k database.Key, v interface{}, _ ...database.QueryOption) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "get")
	span.Tag("key", k.String())
	doc := tx.Table(table).NewDocument(k)
	item, err := tx.reader.Get(doc.PrimaryKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.AsErrNotFound(err)
		}

		return tea.Stack(err)
	}

	if err := item.Value(func(b []byte) error { return doc.Decode(b) }); err != nil {
		return tea.Stack(err)
	}

	return doc.Copy(v)
}
