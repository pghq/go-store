package repository

import (
	"context"

	"github.com/pghq/go-datastore/datastore"
)

// Remove removes items from the repository matching criteria.
func (r *Repository) Remove(ctx context.Context, collection string, filter datastore.Filter, first int) (int, error) {
	command := r.client.Remove().From(collection).Filter(filter)
	if first != 0 {
		command = command.First(first)
	}

	return command.Execute(ctx)
}
