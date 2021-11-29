package ark

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
)

// InMemoryStore implements persistence
type InMemoryStore struct {
	db    *badger.DB
	mutex sync.RWMutex
	ids   map[string]int
}

// NewInMemory creates a new in memory store instance
func NewInMemory() *InMemoryStore {
	db, _ := badger.Open(badger.DefaultOptions("").WithLogger(nil).WithInMemory(true))
	return &InMemoryStore{
		db:  db,
		ids: make(map[string]int),
	}
}

// InMemoryItem is an instance of tagged arbitrary data
type InMemoryItem struct {
	Id   int
	Data []byte
}

// Bytes encoded item
func (i *InMemoryItem) Bytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&i)
	return buf.Bytes()
}

// SetValue for item
func (i *InMemoryItem) SetValue(v interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return tea.Error(err)
	}
	i.Data = buf.Bytes()
	return nil
}

// Value decodes the item
func (i *InMemoryItem) Value(v interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(i.Data))
	return dec.Decode(v)
}
