package inmem

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

// DB In-memory database
// todo: auto detect primary (SQL?)
type DB struct {
	backend *badger.DB
	schema  db.Schema
	tables  map[string]Table
}

func (d DB) Ping(_ context.Context) error { return nil }

// Table Get a table by name
func (d DB) Table(name string) Table {
	if name == "" && d.schema == nil {
		return Table{}
	}

	tbl, present := d.tables[name]
	if !present {
		return Table{index: prefix([]byte(name))}
	}

	return tbl
}

// NewDB Create a new in-memory database
func NewDB(opts ...db.Option) *DB {
	config := db.ConfigWith(opts)
	d := DB{
		tables: make(map[string]Table),
		schema: config.Schema,
	}

	d.backend, _ = badger.Open(badger.DefaultOptions("").
		WithLogger(nil).
		WithInMemory(true).
		WithNamespaceOffset(0),
	)

	for key, indexes := range d.schema {
		d.tables[key] = Table{
			subIndices: make(map[string]SubIndex),
			index:      prefix([]byte(key)),
		}

		for indexName, columns := range indexes {
			d.tables[key].subIndices[indexName] = SubIndex{
				Parent:  d.tables[key].index,
				Name:    indexName,
				Columns: columns,
			}
		}
	}

	return &d
}

// Table in-memory table
type Table struct {
	index      []byte
	subIndices map[string]SubIndex
}

// IteratorOptions gets the iterator options from the query
func (tbl Table) IteratorOptions(query db.Query) IteratorOptions {
	io := IteratorOptions{
		badger: badger.DefaultIteratorOptions,
	}
	io.badger.Prefix = tbl.index
	if len(query.Eq) > 0 {
		for _, eq := range query.Eq {
			for indexName, indexValue := range eq {
				if index, err := tbl.NewIndex(indexName, indexValue); err == nil {
					io.badger.Prefix = index.Index
					io.indexing = true
					break
				}
			}

			if io.indexing {
				break
			}
		}
	}

	return io
}

// NewDocument Get indexes and encoded bytes for arbitrary values
func (tbl Table) NewDocument(k interface{}) *Document {
	return NewDocument(tbl, k)
}

// NewIndex builds an index
func (tbl Table) NewIndex(name string, v interface{}) (SubIndex, error) {
	idx, present := tbl.subIndices[name]
	if !present {
		return SubIndex{}, tea.NewError("index not found")
	}
	return idx.Build(v), nil
}

// IteratorOptions is a configuration for listing
type IteratorOptions struct {
	indexing bool
	badger   badger.IteratorOptions
}

// Document in-memory representation of value
type Document struct {
	PrimaryKey   []byte
	AttributeKey []byte
	Key          []byte
	Value        []byte
	Matcher      Attributes
	table        Table
}

// SetValue sets a documents value
func (doc *Document) SetValue(v interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return tea.Error(err)
	}
	doc.Value = buf.Bytes()
	doc.Matcher = NewAttributes(v, doc.table.subIndices)
	return nil
}

// Bytes encodes the document to bytes
func (doc *Document) Bytes() []byte {
	b, _ := db.Encode(doc)
	return b
}

// Decode bytes into document
func (doc *Document) Decode(b []byte) error {
	if err := db.Decode(b, doc); err != nil {
		return tea.Error(err)
	}

	return nil
}

// Copy document to value
func (doc *Document) Copy(v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(doc.Value)).Decode(v)
}

// Matches checks if query matches document
func (doc *Document) Matches(query db.Query, v interface{}) bool {
	_ = doc.Copy(v)
	m, _ := db.Map(v)
	return doc.Matcher.Contains(query, m)
}

// NewDocument creates a new document instance
func NewDocument(table Table, k interface{}) *Document {
	doc := Document{
		Key:   []byte(fmt.Sprintf("%s", k)),
		table: table,
	}

	doc.PrimaryKey = append(table.index, doc.Key...)
	doc.AttributeKey = append(prefix(table.index, []byte{1}), doc.Key...)
	return &doc
}

// Attributes is a sub-index matcher
type Attributes map[string]SubIndex

// Contains checks if the query contains a value
func (a Attributes) Contains(query db.Query, hash map[string]interface{}) bool {
	if !query.HasFilter() {
		return true
	}

	for _, eq := range query.Eq {
		for k, v := range eq {
			if index, present := a[k]; present {
				if !index.Equal(hash, v) {
					return false
				}
				continue
			}

			value, _ := hash[k]
			if value != v {
				return false
			}
		}
	}

	for _, neq := range query.NotEq {
		for k, v := range neq {
			if index, present := a[k]; present {
				if index.Equal(hash, v) {
					return false
				}
				continue
			}

			value, _ := hash[k]
			if value == v {
				return false
			}
		}
	}

	//for _, gt := range query.Gt{
	//	for k, v := range gt{
	//		value, _ := hash[k]
	//		if value < v{
	//			return false
	//		}
	//	}
	//}

	return true
}

// NewAttributes creates a new index matcher
func NewAttributes(v interface{}, attributes map[string]SubIndex) Attributes {
	a := make(Attributes)
	hash, _ := db.Map(v)
	if len(hash) > 0 && len(attributes) > 0 {
		for _, idx := range attributes {
			a[idx.Name] = idx.Build(hash)
		}
	}
	return a
}

// SubIndex for faster filtering
type SubIndex struct {
	Parent  []byte
	Name    string
	Columns []string
	Index   []byte
}

func (i SubIndex) Equal(expected, actual interface{}) bool {
	ex := i.ValueOf(expected)
	ax := i.ValueOf(actual)
	match := true
	for i := 0; i < len(ex) && i < len(ax); i++ {
		if ex[i] != ax[i] {
			match = false
		}
	}
	return len(ex) == len(ax) && match
}

// ValueOf gets the value of the index if present
func (i SubIndex) ValueOf(v interface{}) []interface{} {
	values := make([]interface{}, int(math.Max(float64(len(i.Columns)), 1)))
	switch vt := v.(type) {
	case map[string]interface{}:
		isNil := true
		for x, column := range i.Columns {
			cv, present := vt[column]
			values[x] = cv
			if present {
				isNil = false
			}
		}

		if isNil {
			return nil
		}
	case []interface{}:
		for x := 0; x < len(i.Columns) && x < len(vt); x++ {
			values[x] = vt[x]
		}
	default:
		values[0] = v
	}

	return values
}

// Build builds an index for a value
func (i SubIndex) Build(v interface{}) SubIndex {
	i.Index = prefix(i.Parent, i.Name, i.ValueOf(v))
	return i
}

// Key get cache key for index
func (i SubIndex) Key(pk []byte) []byte {
	return append(i.Index, pk...)
}

// prefix fixed size prefix for keys
func prefix(base []byte, args ...interface{}) []byte {
	if len(args) > 0 {
		base = append(base, []byte(fmt.Sprintf("%s", args))...)
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, xxhash.Sum64(base))
	return prefix
}
