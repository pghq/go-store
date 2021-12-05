package internal

import (
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
)

var _ Stmt = List{}

// List value(s)
type List struct {
	Table     string
	LeftJoins []LeftJoin
	After     After
	OrderBy   []string
	Limit     int
	Key       []byte
	Filter    interface{}
	Fields    []interface{}
	FieldFunc func(string) string
}

func (l List) Bytes() []byte {
	prefix := []byte(fmt.Sprintf("list:%s:%v:%v:%v:%v:%v:%d:",
		l.Table, l.LeftJoins, l.Fields, l.Filter, l.After, l.OrderBy, l.Limit))
	return append(prefix, l.Key...)
}

func (l List) SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		Select().
		From(l.Table).
		OrderBy(l.OrderBy...)

	if pp != ""{
		builder = builder.PlaceholderFormat(pp)
	}

	if l.Limit > 0 {
		builder = builder.Limit(uint64(l.Limit))
	}

	if l.After.Key != "" && l.After.Value != nil && !l.After.Value.IsZero() {
		builder = builder.Where(squirrel.Gt{l.After.Key: l.After.Value})
	}

	if l.Filter != nil {
		builder = builder.Where(l.Filter)
	}

	for _, j := range l.LeftJoins {
		builder = builder.LeftJoin(j.Join, j.Args...)
	}

	fields := Fields(l.Fields...)
	for _, field := range fields {
		field = ToSnakeCase(field)
		if l.FieldFunc != nil {
			field = l.FieldFunc(field)
		}

		builder = builder.Column(field)
	}

	return builder.ToSql()
}

func (l List) StandardMethod() StandardMethod {
	return StandardMethod{
		List:   true,
		Table:  l.Table,
		Key:    l.Key,
		Filter: l.Filter,
	}
}

type LeftJoin struct {
	Join string
	Args []interface{}
}

type After struct {
	Key   string
	Value *time.Time
}
