package rdb

import (
	"bytes"
	"context"
	"reflect"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
	"github.com/pghq/go-ark/internal/compress"
)

func (p *Provider) Txn(ctx context.Context, ro ...bool) (internal.Txn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	txn := rdbTxn{
		ctx:      ctx,
		btx:      p.client.NewTransaction(len(ro) == 0 || ro[0] == false),
		provider: p,
	}

	return &txn, nil
}

// rdbTxn is a database transaction for the in-memory RDB provider
type rdbTxn struct {
	ctx      context.Context
	btx      *badger.Txn
	provider *Provider
}

func (t *rdbTxn) Exec(statement internal.Stmt, args ...interface{}) internal.Resolver {
	m := statement.StandardMethod()
	if m.Filter != nil {
		if _, ok := m.Filter.(Filter); !ok {
			return internal.ExecResponse(0, tea.NewError("bad filter"))
		}
	}

	if (m.Insert || m.Update) && m.Value == nil {
		return internal.ExecResponse(0, tea.NewError("missing value"))
	}

	if (m.Get || m.List) && len(args) == 0 {
		return internal.ExecResponse(0, tea.NewError("missing destination"))
	}

	var res internal.Resolver
	switch {
	case m.Insert:
		res = t.insert(m.Table, m.Value)
	case m.Update:
		res = t.update(m.Table, m.Filter, m.Value)
	case m.Get:
		res = t.get(m.Table, m.Filter, args[0])
	case m.List:
		res = t.list(m.Table, m.Filter, args[0])
	case m.Remove:
		res = t.remove(m.Table, m.Filter)
	}
	return res
}

func (t *rdbTxn) Commit() error {
	return t.btx.Commit()

}
func (t *rdbTxn) Rollback() error {
	t.btx.Discard()
	return nil
}

// insert value
func (t *rdbTxn) insert(tableName string, v interface{}) internal.Resolver {
	table, err := t.provider.table(tableName)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	primary, secondary, err := table.toDocument(v)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	pk := key(primary.name, primary.value)
	if _, err := t.btx.Get(pk); err == nil {
		return internal.ExecResponse(0, tea.NewBadRequest("unique constraint violation"))
	}

	err = t.insertSecondary(pk, secondary)
	if err == nil {
		err = t.btx.Set(pk, value(v))
	}

	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	return internal.ExecResponse(1, nil)
}

// insert secondary indexes
func (t *rdbTxn) insertSecondary(pk []byte, secondary []*indexValue) error {
	var keys [][]byte
	for _, index := range secondary {
		key := key(index.name, pk, index.value)
		keys = append(keys, key)
		if err := t.btx.Set(key, nil); err != nil {
			return tea.Error(err)
		}
	}

	return t.btx.Set(append(pk, []byte("secondary")...), value(keys))
}

// update value
func (t *rdbTxn) update(tableName string, f, v interface{}) internal.Resolver {
	table, err := t.provider.table(tableName)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	filter, _ := f.(Filter)
	index, err := table.index(filter.index)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	if index.name != "primary" {
		return internal.ExecResponse(0, tea.NewError("bad op"))
	}

	pk := key(index.name, filter.args)
	if _, err := t.btx.Get(pk); err != nil {
		return internal.ExecResponse(0, tea.NewBadRequest("unique constraint violation"))
	}

	t.remove(tableName, f)
	return t.insert(tableName, v)
}

// remove value
func (t *rdbTxn) remove(tableName string, f interface{}) internal.Resolver {
	table, err := t.provider.table(tableName)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	filter, _ := f.(Filter)
	index, err := table.index(filter.index)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	if index.name != "primary" {
		return internal.ExecResponse(0, tea.NewError("bad op"))
	}

	pk := key(index.name, filter.args)
	err = t.btx.Delete(pk)
	if err == nil {
		err = t.removeSecondary(pk)
	}

	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	return internal.ExecResponse(1, nil)
}

// removeSecondary indexes for pk
func (t *rdbTxn) removeSecondary(pk []byte) error {
	var secondary [][]byte
	item, err := t.btx.Get(append(pk, []byte("secondary")...))
	if err == nil {
		err = item.Value(func(b []byte) error { return compress.BrotliDecode(b, &secondary) })
	}

	for _, key := range secondary {
		_ = t.btx.Delete(key)
	}

	return err
}

// get value
func (t *rdbTxn) get(tableName string, f interface{}, v interface{}) internal.Resolver {
	table, err := t.provider.table(tableName)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	filter, _ := f.(Filter)
	index, err := table.index(filter.index)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	if index.name != "primary" {
		return internal.ExecResponse(0, tea.NewError("bad op"))
	}

	pk := key(index.name, filter.args)
	item, err := t.btx.Get(pk)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = tea.NewError(err)
		}

		return internal.ExecResponse(0, tea.Error(err))
	}

	if err := item.Value(func(b []byte) error { return compress.BrotliDecode(b, v) }); err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	return internal.ExecResponse(1, nil)
}

// list values
func (t *rdbTxn) list(tableName string, f interface{}, v interface{}) internal.Resolver {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return internal.ExecResponse(0, tea.NewError("dst must be a pointer"))
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return internal.ExecResponse(0, tea.NewError("dst must be a pointer to slice"))
	}

	table, err := t.provider.table(tableName)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	filter, _ := f.(Filter)
	index, err := table.index(filter.index)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	if index.name == "primary" {
		return internal.ExecResponse(0, tea.NewError("bad op"))
	}

	ns := namespace(index.name, filter.args)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = ns
	it := t.btx.NewIterator(opts)
	defer it.Close()

	var values []reflect.Value
	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Item().Key()
		pk := bytes.TrimPrefix(key, ns)
		item, err := t.btx.Get(pk)
		if err == nil {
			rv := reflect.New(reflect.TypeOf(v).Elem().Elem())
			if err := item.Value(func(b []byte) error { return compress.BrotliDecode(b, &rv) }); err != nil {
				return internal.ExecResponse(0, tea.Error(err))
			}
			values = append(values, rv.Elem())
		}
	}

	rv.Set(reflect.Append(rv, values...))
	return internal.ExecResponse(len(values), nil)
}
