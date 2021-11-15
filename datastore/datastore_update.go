package datastore

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Update updates items matching filters
func (ctx *Context) Update(collection string, filter, data interface{}) (int, error) {
	item, err := Item(data)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	command := ctx.repo.client.Update().In(collection).Filter(filter).Item(item)
	count, err := ctx.tx.Execute(command)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	return count, nil
}
