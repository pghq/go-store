package internal

import (
	"fmt"

	"github.com/Masterminds/squirrel"
)

var _ Stmt = Get{}

// Get value
type Get struct {
	Table     string
	Key       []byte
	Filter    interface{}
	Fields    []interface{}
	FieldFunc func(string) string
}

func (g Get) Bytes() []byte {
	return append([]byte(fmt.Sprintf("get:%s:%v:%v:", g.Table, g.Fields, g.Filter)), g.Key...)
}

func (g Get) SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		Select().
		From(g.Table).
		Limit(1).
		Where(g.Filter)

	if pp != "" {
		builder = builder.PlaceholderFormat(pp)
	}

	fields := Fields(g.Fields...)
	for _, field := range fields {
		field = ToSnakeCase(field)
		if g.FieldFunc != nil {
			field = g.FieldFunc(field)
		}

		builder = builder.Column(field)
	}

	return builder.ToSql()
}

func (g Get) StandardMethod() StandardMethod {
	return StandardMethod{
		Get:    true,
		Table:  g.Table,
		Key:    g.Key,
		Filter: g.Filter,
	}
}
