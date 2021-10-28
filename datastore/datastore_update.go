package datastore

import (
	"context"

	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Update updates an item matching a filter
func (r *Repository) Update(ctx context.Context, collection string, filter client.Filter, i interface{}) (int, error) {
	item, err := r.item(i)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	return r.client.Update().In(collection).Filter(filter).Item(item).Execute(ctx)
}
