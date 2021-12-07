package rdb

import (
	"context"

	"github.com/hashicorp/go-memdb"
	"github.com/pghq/go-tea"
)

// Provider is a low-level in-memory RDB provider.
type Provider struct {
	client *memdb.MemDB
	schema memdb.DBSchema
}

// NewProvider creates a new in-memory RDB provider
func NewProvider(source interface{}) *Provider {
	schema, _ := source.(memdb.DBSchema)
	return &Provider{
		schema: schema,
	}
}

func (p *Provider) Connect(_ context.Context) error {
	client, err := memdb.NewMemDB(&p.schema)
	if err != nil {
		return tea.Error(err)
	}
	p.client = client
	return nil
}

// defaultIndex for unspecified filters
func (p *Provider) defaultIndex(table string) string {
	var index string
	if ts, present := p.schema.Tables[table]; present {
		for i := range ts.Indexes {
			index = i
			break
		}
	}

	return index
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
