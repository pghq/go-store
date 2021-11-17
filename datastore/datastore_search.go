package datastore

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Query gets a new query for searching the repository.
func (r *Repository) Query() client.Query {
	return r.client.Query()
}

// Search retrieves items from the repository matching criteria.
func (ctx *Context) Search(query client.Query, dst interface{}) error {
	_, err := ctx.tx.Execute(query, dst)
	if err != nil {
		return errors.Wrap(err)
	}

	return nil
}
