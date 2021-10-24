package datastore

import (
	"context"

	"github.com/pghq/go-datastore/datastore/client"
)

// Update updates an item matching a filter
func (r *Repository) Update(ctx context.Context, collection string, filter client.Filter, item client.Snapper) (int, error) {
	return r.client.Update().In(collection).Filter(filter).Item(item.Snapshot()).Execute(ctx)
}
