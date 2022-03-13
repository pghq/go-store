package memory

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
)

func (tx txn) Get(table string, k, v interface{}, _ ...interface{}) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "get")
	span.Tag("key", fmt.Sprintf("%s", k))
	doc := tx.Table(table).NewDocument(k)
	item, err := tx.reader.Get(doc.PrimaryKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return tea.AsErrNotFound(err)
		}

		return tea.Stacktrace(err)
	}

	if err := item.Value(func(b []byte) error { return doc.Decode(b) }); err != nil {
		return tea.Stacktrace(err)
	}

	return doc.Copy(v)
}
