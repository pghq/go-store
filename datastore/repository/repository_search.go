package repository

import (
	"context"

	"github.com/pghq/go-datastore/datastore"
)

// Query gets a new query for searching the repository.
func (r *Repository) Query() datastore.Query {
	return r.client.Query()
}

// Search retrieves items from the repository matching criteria.
func (r *Repository) Search(ctx context.Context, query datastore.Query) (datastore.Cursor, error) {
	return query.Execute(ctx)
}
