package datastore

// Remove removes items from the repository matching criteria.
func (ctx *Context) Remove(collection string, filter interface{}) (int, error) {
	return ctx.tx.Execute(ctx.repo.client.Remove().From(collection).Filter(filter))
}
