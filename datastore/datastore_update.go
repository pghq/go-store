package datastore

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Update updates items matching filters
func (ctx *Context) Update(collection string, filter client.Filter, data interface{}, first int) (int, error) {
	item, err := ctx.repo.item(data)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	command := ctx.repo.client.Update().In(collection).Filter(filter).First(first).Item(item)
	count, err := ctx.tx.Execute(command)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	return count, nil
}
