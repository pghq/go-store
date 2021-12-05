package rdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
)

func (p *Provider) Txn(ctx context.Context, ro ...bool) (internal.Txn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	txn := rdbTxn{
		ctx:          ctx,
		provider:     p.client.Txn(len(ro) == 0 || ro[0] == false),
		defaultIndex: p.defaultIndex,
	}

	return &txn, nil
}

// rdbTxn is a database transaction for the in-memory RDB provider
type rdbTxn struct {
	ctx          context.Context
	provider     *memdb.Txn
	defaultIndex func(string) string
}

func (t *rdbTxn) Exec(statement internal.Stmt, args ...interface{}) internal.Resolver {
	m := statement.StandardMethod()
	if m.Filter == nil && (m.Update || m.Get || m.Remove) {
		return internal.ExecResponse(0, tea.NewError("missing filter"))
	}

	if m.Filter != nil {
		if _, ok := m.Filter.(Filter); !ok {
			return internal.ExecResponse(0, tea.NewError("bad filter"))
		}
	}

	if (m.Get || m.List) && len(args) == 0 {
		return internal.ExecResponse(0, tea.NewError("missing destination"))
	}

	value := m.Value
	if m.Insert || m.Update {
		rv := reflect.ValueOf(value)
		if rv.IsValid() && rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
			value = rv.Interface()
		}

		if !rv.IsValid() || rv.Kind() != reflect.Struct {
			return internal.ExecResponse(0, tea.NewError("value must be a struct or pointer to struct"))
		}
	}

	if m.Insert {
		if err := t.provider.Insert(m.Table, value); err != nil {
			return internal.ExecResponse(0, tea.Error(err))
		}
		return internal.ExecResponse(1, nil)
	}

	var items []interface{}
	ft, ok := m.Filter.(Filter)
	if !ok {
		ft = Filter{}.IdxEq(t.defaultIndex(m.Table))
	}

	if ft.prefix {
		ft.index = fmt.Sprintf("%s_prefix", ft.index)
	}

	it, err := t.provider.Get(m.Table, ft.index, ft.args...)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		items = append(items, obj)
	}

	if !m.Remove && len(items) == 0 {
		return internal.ExecResponse(0, tea.NewNoContent("not found"))
	}

	for _, item := range items {
		if m.Update || m.Remove {
			if err := t.provider.Delete(m.Table, item); err != nil {
				return internal.ExecResponse(0, tea.Error(err))
			}
		}

		if m.Update {
			if err := t.provider.Insert(m.Table, m.Value); err != nil {
				return internal.ExecResponse(0, tea.Error(err))
			}
		}
	}

	if m.Get || m.List {
		dv := reflect.ValueOf(args[0])
		if dv.Kind() != reflect.Ptr || dv.IsNil() || !dv.IsValid() {
			return internal.ExecResponse(0, tea.NewError("dst must be a pointer"))
		}

		rv := dv.Elem()
		if m.Get {
			if rv.Kind() != reflect.Struct {
				return internal.ExecResponse(0, tea.NewError("dst must be a pointer to struct"))
			}
			rv.Set(reflect.ValueOf(items[0]))
			return internal.ExecResponse(1, nil)
		}

		if rv.Kind() != reflect.Slice {
			return internal.ExecResponse(0, tea.NewError("dst must be a pointer to slice"))
		}

		var values []reflect.Value
		for _, item := range items {
			values = append(values, reflect.ValueOf(item))
		}

		rv.Set(reflect.Append(rv, values...))
	}

	return internal.ExecResponse(len(items), nil)
}

func (t *rdbTxn) Commit() error {
	t.provider.Commit()
	return nil

}
func (t *rdbTxn) Rollback() error {
	t.provider.Abort()
	return nil
}
