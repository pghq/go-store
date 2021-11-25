package ark

import (
	"github.com/pghq/go-tea"
)

// Update updates items matching filters
func (ctx *Context) Update(collection string, filter, data interface{}) (int, error) {
	item, err := Item(data)
	if err != nil {
		return 0, tea.Error(err)
	}

	return ctx.tx.Execute(ctx.store.client.Update().In(collection).Filter(filter).Item(item))
}
