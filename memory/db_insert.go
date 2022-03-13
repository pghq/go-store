package memory

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, k, v interface{}, args ...interface{}) error {
	doc := tx.Table(table).NewDocument(k)
	if err := doc.SetValue(v); err != nil {
		return tea.Stacktrace(err)
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "insert")
	span.Tag("key", fmt.Sprintf("%s", k))
	span.Tag("value", v)

	var indexes [][]byte
	req := database.NewRequest(args...)
	for _, index := range doc.Matcher {
		key := index.Key(doc.PrimaryKey)
		entry := badger.NewEntry(key, nil)
		if req.Expire {
			entry = entry.WithTTL(req.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Stacktrace(err)
		}

		indexes = append(indexes, key)
	}

	if len(indexes) > 0 {
		b, _ := database.Encode(indexes)
		entry := badger.NewEntry(doc.AttributeKey, b)
		if req.Expire {
			entry = entry.WithTTL(req.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Stacktrace(err)
		}
	}

	entry := badger.NewEntry(doc.PrimaryKey, doc.Bytes())
	if req.Expire {
		entry = entry.WithTTL(req.TTL)
	}

	return tx.writer.SetEntry(entry)
}
