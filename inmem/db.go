package inmem

import (
	"context"
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// DB In-memory database
type DB struct {
	backend *badger.DB
	schema  db.Schema
	tables  map[string]table
}

func (d DB) Ping(_ context.Context) error { return nil }

// table Get a table by name
func (d DB) table(name string) (table, error) {
	if name == "" && d.schema == nil {
		return table{}, nil
	}

	tbl, present := d.tables[name]
	if !present {
		return table{}, tea.NewError("table not found")
	}

	return tbl, nil
}

// NewDB Create a new in-memory database
func NewDB(opts ...db.Option) *DB {
	config := db.ConfigWith(opts)
	d := DB{
		tables: make(map[string]table),
		schema: config.Schema,
	}

	d.backend, _ = badger.Open(badger.DefaultOptions("").
		WithLogger(nil).
		WithInMemory(true).
		WithNamespaceOffset(0),
	)

	for key, indexes := range d.schema {
		tbl := table{indexes: make(map[string]index)}
		tbl.prefix, _ = prefix([]byte(key))
		pm := primary{}
		pm.prefix, _ = prefix([]byte(key), "id")
		tbl.primary = pm
		d.tables[key] = tbl

		for indexName, columns := range indexes {
			d.tables[key].indexes[indexName] = index{
				name:    indexName,
				primary: d.tables[key].primary,
				columns: columns,
			}
		}
	}

	return &d
}

// table in-memory table
type table struct {
	primary primary
	prefix  []byte
	indexes map[string]index
}

// document Get indexes and encoded bytes for arbitrary values
func (tbl table) document(v interface{}) (document, error) {
	doc := document{}
	if len(tbl.indexes) > 0 {
		m, err := db.Map(v)
		if err != nil {
			return document{}, tea.Error(err)
		}

		for _, idx := range tbl.indexes {
			if err := idx.build(m); err != nil {
				if tea.IsFatal(err) {
					return document{}, tea.Error(err)
				}
				continue
			}
			doc.indexes = append(doc.indexes, idx)
		}
	}

	value, err := db.Encode(v)
	if err != nil {
		return document{}, tea.Error(err)
	}

	doc.value = value
	return doc, nil
}

// index lookup an index in a value
func (tbl table) index(name string, v interface{}) (index, error) {
	idx, present := tbl.indexes[name]
	if !present {
		return index{}, tea.NewNotFound("index not found")
	}

	if err := idx.build(v); err != nil {
		return index{}, tea.Error(err)
	}

	return idx, nil
}

// document in-memory representation of value
type document struct {
	m       map[string]interface{}
	value   []byte
	indexes []index
}

// primary primary index for documents
type primary struct {
	prefix []byte
}

// pk get primary key
func (p primary) pk(k []byte) []byte {
	return append(p.prefix, append([]byte{0}, k...)...)
}

// ck get composite key
func (p primary) ck(k []byte) []byte {
	return append(p.prefix, append([]byte{1}, k...)...)
}

// index index identifying structure of documents used for querying data
type index struct {
	primary primary
	name    string
	columns []string
	prefix  []byte
}

// build get all index properties from value
func (i *index) build(v interface{}) error {
	values := make([]interface{}, len(i.columns))
	switch vt := v.(type) {
	case map[string]interface{}:
		isNil := true
		for x, column := range i.columns {
			cv, present := vt[column]
			values[x] = cv
			if present {
				isNil = false
			}
		}

		if isNil {
			return tea.NewNotFound("index not found")
		}
	case []interface{}:
		for x := 0; x < len(i.columns) && x < len(vt); x++ {
			values[x] = vt[x]
		}
	default:
		values[0] = v
	}

	pfx, err := prefix([]byte(i.name), values)
	if err != nil {
		return tea.Error(err)
	}

	i.prefix = pfx
	return nil
}

// key get cache key for index
func (i index) key(pk []byte) []byte {
	return append(i.prefix, pk...)
}

// prefix fixed size prefix for keys
func prefix(base []byte, args ...interface{}) ([]byte, error) {
	if len(args) > 0 {
		b, err := db.Hash(args...)
		if err != nil {
			return nil, tea.Error(err)
		}

		base = append(base, b...)
	}

	table := make([]byte, 8)
	binary.LittleEndian.PutUint64(table, xxhash.Sum64(base))
	return table, nil
}
