package internal

import (
	"fmt"

	"github.com/Masterminds/squirrel"
)

var _ Stmt = Remove{}

// Remove value(s)
type Remove struct {
	Table   string
	Key     []byte
	Filter  interface{}
	OrderBy []string
}

func (r Remove) Bytes() []byte {
	return append([]byte(fmt.Sprintf("remove:%s:%v:%v:", r.Table, r.Filter, r.OrderBy)), r.Key...)
}

func (r Remove) SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		Delete(r.Table).
		OrderBy(r.OrderBy...)

	if pp != ""{
		builder = builder.PlaceholderFormat(pp)
	}

	if r.Filter != nil {
		builder = builder.Where(r.Filter)
	}

	return builder.ToSql()
}

func (r Remove) StandardMethod() StandardMethod {
	return StandardMethod{
		Remove: true,
		Table:  r.Table,
		Key:    r.Key,
		Filter: r.Filter,
	}
}
