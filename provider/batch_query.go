package provider

// BatchQueryItem a single batch query
type BatchQueryItem struct {
	Spec     Spec
	Value    interface{}
	One      bool
	Skip     bool
	Optional bool
}

// BatchQuery a list of batch query items
type BatchQuery []*BatchQueryItem

// One append a query expecting a single result
func (b *BatchQuery) One(spec Spec, v interface{}, opts ...BatchQueryOption) {
	item := BatchQueryItem{
		Spec:  spec,
		Value: v,
		One:   true,
	}

	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// All append a query expecting multiple results
func (b *BatchQuery) All(spec Spec, v interface{}, opts ...BatchQueryOption) {
	item := BatchQueryItem{
		Spec:  spec,
		Value: v,
	}

	for _, opt := range opts {
		opt(&item)
	}

	*b = append(*b, &item)
}

// BatchQueryOption for custom query configuration
type BatchQueryOption func(item *BatchQueryItem)

// WithBatchItemOptional marks the item as optional ignoring client errors
func WithBatchItemOptional(flag bool) BatchQueryOption {
	return func(item *BatchQueryItem) {
		item.Optional = flag
	}
}
