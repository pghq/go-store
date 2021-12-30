package redis

import (
	"fmt"
	"reflect"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) List(table string, v interface{}, args ...interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return tea.Err("dst must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return tea.Err("dst must be a pointer to slice")
	}

	req := database.NewRequest(args...)
	cmd := tx.unit.Scan(tx.ctx, uint64(req.Page), fmt.Sprintf("%s.*", table), int64(req.Limit))
	span := tea.Nest(tx.ctx, "redis")
	defer span.End()
	span.Tag("statement", cmd.String())
	select {
	case tx.reads <- read{cmd: cmd, v: v, limit: req.Limit}:
	default:
		return tea.Err("read batch size exhausted")
	}

	return nil
}
