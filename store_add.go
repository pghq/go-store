package ark

import (
	"github.com/pghq/go-tea"
)

// Add adds item to the repository
func (ctx *Context) Add(collection string, data interface{}) error {
	item, err := Item(data)
	if err != nil {
		return tea.Error(err)
	}

	command := ctx.store.client.Add().To(collection).Item(item)
	if _, err := ctx.tx.Execute(command); err != nil {
		return tea.Error(err)
	}

	return nil
}
