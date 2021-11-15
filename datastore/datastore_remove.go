package datastore

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Remove removes items from the repository matching criteria.
func (ctx *Context) Remove(collection string, filter interface{}) (int, error) {
	command := ctx.repo.client.Remove().From(collection).Filter(filter)
	count, err := ctx.tx.Execute(command)
	if err != nil {
		return 0, errors.Wrap(err)
	}

	return count, nil
}
