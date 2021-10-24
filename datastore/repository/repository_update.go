package repository

import (
	"context"

	"github.com/pghq/go-datastore/datastore"
)

// Update updates an item matching a filter
func (r *Repository) Update(ctx context.Context, collection string, filter datastore.Filter, item datastore.Snapper) (int, error) {
	return r.client.Update().In(collection).Filter(filter).Item(item.Snapshot()).Execute(ctx)
}
