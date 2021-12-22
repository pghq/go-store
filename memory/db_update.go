package memory

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Update(table string, k database.Key, v interface{}, opts ...database.CommandOption) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "update")
	span.Tag("key", k.String())
	doc := tx.Table(table).NewDocument(k)
	if _, err := tx.reader.Get(doc.PrimaryKey); err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.AsErrNotFound(err)
		}

		return tea.Stack(err)
	}

	return tx.Insert(table, k, v, opts...)
}
