package db

import (
	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-store/enc"
)

// BatchItem a single batch query
type BatchItem struct {
	Spec     Spec
	Value    interface{}
	One      bool
	Skip     bool
	Optional bool
	Defer    bool
}

// Batch a list of batch query items
type Batch []*BatchItem

// Add an item
func (b *Batch) Add(collection string, v interface{}, opts ...BatchOption) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert(collection).
		SetMap(enc.Map(v))

	item := BatchItem{Spec: Sqlizer(builder)}
	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// Edit an item
func (b *Batch) Edit(collection string, spec Spec, v interface{}, opts ...BatchOption) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update(collection).
		Where(spec).
		SetMap(enc.Map(v))

	item := BatchItem{Spec: Sqlizer(builder)}
	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// One append a query expecting a single result
func (b *Batch) One(spec Spec, v interface{}, opts ...BatchOption) {
	item := BatchItem{
		Spec:  spec,
		Value: v,
		One:   true,
	}

	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// Do append a query expecting a single result
func (b *Batch) Do(spec Spec, opts ...BatchOption) {
	item := BatchItem{
		Spec: spec,
	}

	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// All append a query expecting multiple results
func (b *Batch) All(spec Spec, v interface{}, opts ...BatchOption) {
	item := BatchItem{
		Spec:  spec,
		Value: v,
	}

	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// BatchOption for custom query configuration
type BatchOption func(item *BatchItem)

// Optional marks the item as optional ignoring client errors
func Optional(flag bool) BatchOption {
	return func(item *BatchItem) {
		item.Optional = flag
	}
}
