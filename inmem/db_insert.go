package inmem

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Insert(table, k string, v interface{}, opts ...db.CommandOption) error {
	cmd := db.CommandWith(opts)
	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	doc, err := tbl.document(v)
	if err != nil {
		return tea.Error(err)
	}

	pk := tbl.primary.pk([]byte(k))
	var composite [][]byte
	for _, index := range doc.indexes {
		key := index.key(pk)
		entry := badger.NewEntry(key, nil)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err := tx.set(entry); err != nil {
			return tea.Error(err)
		}

		composite = append(composite, key)
	}

	if len(composite) > 0 {
		b, _ := db.Encode(composite)
		entry := badger.NewEntry(tbl.primary.ck([]byte(k)), b)
		if cmd.Expire {
			entry = entry.WithTTL(cmd.TTL)
		}

		if err = tx.set(entry); err != nil {
			return tea.Error(err)
		}
	}

	entry := badger.NewEntry(pk, doc.value)
	if cmd.Expire {
		entry = entry.WithTTL(cmd.TTL)
	}

	return tx.set(entry)
}
