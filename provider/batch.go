package provider

// BatchItem a single batch query
type BatchItem struct {
	Spec     Spec
	Value    interface{}
	One      bool
	Skip     bool
	Optional bool
}

// Batch a list of batch query items
type Batch []*BatchItem

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

// WithBatchItemOptional marks the item as optional ignoring client errors
func WithBatchItemOptional(flag bool) BatchOption {
	return func(item *BatchItem) {
		item.Optional = flag
	}
}
