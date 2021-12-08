package rdb

import (
	"context"
	"encoding/binary"

	"github.com/cespare/xxhash"
	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
	"github.com/pghq/go-ark/internal/z"
)

// Provider is a low-level in-memory RDB provider.
type Provider struct {
	client *badger.DB
	schema Schema
	tables map[string]*table
}

// NewProvider creates a new in-memory RDB provider
func NewProvider(source interface{}) *Provider {
	schema, _ := source.(Schema)
	return &Provider{
		schema: schema,
		tables: make(map[string]*table),
	}
}

func (p *Provider) Connect(_ context.Context) error {
	if err := p.schema.Validate(); err != nil {
		return tea.Error(err)
	}

	client, err := badger.Open(badger.DefaultOptions("").WithLogger(nil).WithInMemory(true).WithNamespaceOffset(0))
	p.client = client

	for tableName, indexes := range p.schema {
		p.tables[tableName] = &table{
			name:      tableName,
			namespace: namespace(tableName),
			client:    p.client,
			indexes:   make(map[string]index),
		}

		for indexName, i := range indexes {
			p.tables[tableName].indexes[indexName] = index{
				name:    indexName,
				columns: i,
			}
		}
	}

	return err
}

func (p *Provider) table(name string) (*table, error) {
	table, present := p.tables[name]
	if !present {
		return nil, tea.NewError("table not found")
	}

	return table, nil
}

// Schema for the db
// e.g., {"table": {"primary": ["column1"]]}}
type Schema map[string]map[string][]string

func (s Schema) Validate() error {
	if len(s) == 0 {
		return tea.NewError("no tables")
	}

	for name, table := range s {
		if _, present := table["primary"]; !present {
			return tea.NewErrorf("no primary for %s", name)
		}
	}

	return nil
}

// Filter is a filter for the in-memory RDB provider
type Filter struct {
	index string
	args  []interface{}
}

// Ft creates an in-memory filter
func Ft() Filter {
	return Filter{}
}

// IdxEq criteria
func (f Filter) IdxEq(index string, args ...interface{}) Filter {
	return Filter{index: index, args: args}
}

// table in the db
type table struct {
	name      string
	namespace []byte
	client    *badger.DB
	indexes   map[string]index
}

// toDocument gets the primary and secondary index values
func (t *table) toDocument(v interface{}) (*indexValue, []*indexValue, error) {
	m, err := internal.ToMap(v, false)
	if err != nil {
		return nil, nil, tea.NewError("bad value")
	}

	primary := t.indexes["primary"].get(m)
	if primary == nil {
		return nil, nil, tea.NewError("missing primary index")
	}

	var secondary []*indexValue
	for k, index := range t.indexes {
		if k != "primary" {
			if v := index.get(m); v != nil {
				secondary = append(secondary, v)
			}
		}
	}

	return primary, secondary, nil
}

// index in the table
func (t *table) index(name string) (*index, error) {
	if index, present := t.indexes[name]; present {
		return &index, nil
	}

	return nil, tea.NewNoContent("index not found")
}

type index struct {
	name    string
	columns []string
}

func (i index) get(v map[string]interface{}) *indexValue {
	values := make([]interface{}, len(i.columns))
	isNil := true
	for x, column := range i.columns {
		cv, present := v[column]
		values[x] = cv
		if present {
			isNil = false
		}
	}

	if isNil {
		return nil
	}

	return &indexValue{
		index: i,
		value: values,
	}
}

type indexValue struct {
	index
	value interface{}
}

// namespace
func namespace(base string, args ...interface{}) []byte {
	raw := []byte(base)
	if len(args) > 0 {
		b, _ := z.Hash(args...)
		raw = append(raw, b...)
	}

	namespace := make([]byte, 8)
	binary.LittleEndian.PutUint64(namespace, xxhash.Sum64(raw))
	return namespace
}

// key
// format: {indexName,args}:value
// e.g., {secondary1_idx,v}:primary1 -> struct{}
// e.g., {primary_idx}:v -> struct{}
func key(indexName string, v interface{}, args ...interface{}) []byte {
	key := namespace(indexName, args...)
	if v != nil {
		key = append(key, value(v)...)
	}

	return key
}

// value
func value(v interface{}) []byte {
	b, ok := v.([]byte)
	if !ok {
		b, _ = z.Encode(v)
	}

	return b
}
