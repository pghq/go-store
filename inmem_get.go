package ark

import (
	"bytes"
	"encoding/gob"
	"strconv"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
)

// Get an item
func (s *InMemoryStore) Get(id interface{}) (*InMemoryItem, error) {
	var key []byte
	switch id := id.(type) {
	case int:
		key = []byte(strconv.Itoa(id))
	case string:
		i, present := s.ids[id]
		if !present {
			return nil, tea.NewNoContent("not found")
		}
		key = []byte(strconv.Itoa(i))
	default:
		return nil, tea.NewError("bad key type")
	}

	var item *InMemoryItem
	return item, s.db.View(func(txn *badger.Txn) error {
		i, err := txn.Get(key)
		if i != nil {
			err = i.Value(func(b []byte) error {
				dec := gob.NewDecoder(bytes.NewReader(b))
				return dec.Decode(&item)
			})
		}
		if err == badger.ErrKeyNotFound {
			err = tea.NoContent(err)
		}
		return err
	})
}
