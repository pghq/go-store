package inmem

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Insert(table string, k, v interface{}, opts ...db.CommandOption) error {
	cmd := db.CommandWith(opts)
	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	doc, err := tbl.document(v)
	if err != nil {
		return tea.Error(err)
	}

	key := []byte(fmt.Sprintf("%s", k))
	pk := tbl.primary.pk(key)
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
		entry := badger.NewEntry(tbl.primary.ck(key), b)
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
