package rdb

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/hashicorp/go-memdb"
	"github.com/pghq/go-tea"
)

// Provider is a low-level in-memory RDB provider.
type Provider struct {
	dsn    string
	client *memdb.MemDB
	schema *memdb.DBSchema
}

// NewProvider creates a new in-memory RDB provider
func NewProvider(dsn string) *Provider {
	return &Provider{
		dsn: dsn,
	}
}

func (p *Provider) Connect(_ context.Context) error {
	var conf RDBConfig
	if err := json.Unmarshal([]byte(p.dsn), &conf); err != nil {
		return tea.Error(err)
	}

	schema := conf.schema()
	client, err := memdb.NewMemDB(schema)
	if err != nil {
		return tea.Error(err)
	}
	p.client = client
	p.schema = schema
	return nil
}

// defaultIndex for unspecified filters
func (p *Provider) defaultIndex(table string) string {
	var index string
	if p.schema != nil {
		if ts, present := p.schema.Tables[table]; present {
			for i := range ts.Indexes {
				index = i
				break
			}
		}
	}

	return index
}

// RDBConfig is a configuration for the in-memory RDB provider
type RDBConfig map[string]RDBTable

// schema for memdb
func (s RDBConfig) schema() *memdb.DBSchema {
	schema := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}

	for tableName, table := range s {
		name := strings.TrimSpace(strings.ToLower(tableName))
		schema.Tables[name] = &memdb.TableSchema{
			Name:    name,
			Indexes: make(map[string]*memdb.IndexSchema),
		}
		for indexName, index := range table {
			memdbIndex := memdb.CompoundIndex{}
			for field, dt := range index.Fields {
				var index memdb.Indexer
				switch dt {
				case "int":
					index = &memdb.IntFieldIndex{
						Field: field,
					}
				case "bool":
					index = &memdb.BoolFieldIndex{
						Field: field,
					}
				default:
					index = &memdb.StringFieldIndex{
						Field: field,
					}
				}

				memdbIndex.Indexes = append(memdbIndex.Indexes, index)
			}

			indexName := strings.TrimSpace(strings.ToLower(indexName))
			schema.Tables[name].Indexes[indexName] = &memdb.IndexSchema{
				Name:         indexName,
				Unique:       index.Unique,
				AllowMissing: !index.Unique,
				Indexer:      &memdbIndex,
			}
		}
	}

	return schema
}

// RDBTable is a table configuration for the in-memory RDB provider
type RDBTable map[string]RDBIndex

// RDBIndex is an index configuration for the in-memory RDB provider
type RDBIndex struct {
	Unique bool              `json:"unique"`
	Fields map[string]string `json:"fields"`
}

// Filter is a filter for the in-memory RDB provider
type Filter struct {
	prefix bool
	index  string
	args   []interface{}
}

// Ft creates an in-memory filter
func Ft() Filter {
	return Filter{}
}

// IdxEq criteria
func (f Filter) IdxEq(index string, args ...interface{}) Filter {
	return Filter{index: index, args: args, prefix: false}
}

// IdxBeginsWith criteria
func (f Filter) IdxBeginsWith(index string, value string) Filter {
	return Filter{index: index, args: []interface{}{value}, prefix: true}
}
