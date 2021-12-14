package redis

import (
	"fmt"
	"reflect"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) List(table string, v interface{}, opts ...db.QueryOption) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return tea.NewError("dst must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return tea.NewError("dst must be a pointer to slice")
	}

	query := db.QueryWith(opts)
	cmd := tx.unit.Scan(tx.ctx, uint64(query.Page), fmt.Sprintf("%s.*", table), int64(query.Limit))
	select {
	case tx.reads <- read{cmd: cmd, v: v, limit: query.Limit}:
	default:
		return tea.NewError("read batch size exhausted")
	}

	return nil
}
