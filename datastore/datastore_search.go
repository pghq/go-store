package datastore

import (
	"context"

	"github.com/pghq/go-datastore/datastore/client"
)

// Query gets a new query for searching the repository.
func (r *Repository) Query() client.Query {
	return r.client.Query()
}

// Search retrieves items from the repository matching criteria.
func (r *Repository) Search(ctx context.Context, query client.Query, dst interface{}) error {
	return query.Execute(ctx, dst)
}
