package memory

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, k database.Key, v interface{}, opts ...database.CommandOption) error {
	doc := tx.Table(table).NewDocument(k)
	if err := doc.SetValue(v); err != nil {
		return tea.Stack(err)
	}

	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "insert")
	span.Tag("key", k.String())
	span.Tag("value", v)

	var indexes [][]byte
	cmd := database.CommandWith(opts)
	for _, index := range doc.Matcher {
		key := index.Key(doc.PrimaryKey)
		entry := badger.NewEntry(key, nil)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Stack(err)
		}

		indexes = append(indexes, key)
	}

	if len(indexes) > 0 {
		b, _ := database.Encode(indexes)
		entry := badger.NewEntry(doc.AttributeKey, b)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Stack(err)
		}
	}

	entry := badger.NewEntry(doc.PrimaryKey, doc.Bytes())
	if cmd.Expire {
		entry = entry.WithTTL(cmd.TTL)
	}

	return tx.writer.SetEntry(entry)
}
