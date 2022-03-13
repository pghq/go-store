package memory

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
)

func (tx txn) Update(table string, k, v interface{}, args ...interface{}) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "update")
	span.Tag("key", fmt.Sprintf("%s", k))
	doc := tx.Table(table).NewDocument(k)
	if _, err := tx.reader.Get(doc.PrimaryKey); err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.AsErrNotFound(err)
		}

		return tea.Stacktrace(err)
	}

	return tx.Insert(table, k, v, args...)
}
