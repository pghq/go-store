package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Insert(table string, k, v interface{}, opts ...db.CommandOption) error {
	doc := tx.Table(table).NewDocument(k)
	if err := doc.SetValue(v); err != nil {
		return tea.Error(err)
	}

	var indexes [][]byte
	cmd := db.CommandWith(opts)
	for _, index := range doc.Matcher {
		key := index.Key(doc.PrimaryKey)
		entry := badger.NewEntry(key, nil)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Error(err)
		}

		indexes = append(indexes, key)
	}

	if len(indexes) > 0 {
		b, _ := db.Encode(indexes)
		entry := badger.NewEntry(doc.AttributeKey, b)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err := tx.writer.SetEntry(entry); err != nil {
			return tea.Error(err)
		}
	}

	entry := badger.NewEntry(doc.PrimaryKey, doc.Bytes())
	if cmd.Expire {
		entry = entry.WithTTL(cmd.TTL)
	}

	return tx.writer.SetEntry(entry)
}
