package datastore

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Add adds item to the repository
func (ctx *Context) Add(collection string, data interface{}) error {
	item, err := Item(data)
	if err != nil {
		return errors.Wrap(err)
	}

	command := ctx.repo.client.Add().To(collection).Item(item)
	if _, err := ctx.tx.Execute(command); err != nil {
		return errors.Wrap(err)
	}

	return nil
}
