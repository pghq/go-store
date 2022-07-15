package provider

// BatchQueryItem a single batch query
type BatchQueryItem struct {
	Spec  Spec
	Value interface{}
	One   bool
	Skip  bool
}

// BatchQuery a list of batch query items
type BatchQuery []*BatchQueryItem

// One append a query expecting a single result
func (b *BatchQuery) One(spec Spec, v interface{}) {
	*b = append(*b, &BatchQueryItem{
		Spec:  spec,
		Value: v,
		One:   true,
	})
}

// All append a query expecting multiple results
func (b *BatchQuery) All(spec Spec, v interface{}) {
	*b = append(*b, &BatchQueryItem{
		Spec:  spec,
		Value: v,
	})
}
