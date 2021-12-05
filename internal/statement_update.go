package internal

import (
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"
)

var _ Stmt = Update{}

// Update item
type Update struct {
	Table  string
	Key    []byte
	Value  interface{}
	Filter interface{}
}

func (u Update) Bytes() []byte {
	return append([]byte(fmt.Sprintf("update:%s:%v:%v:", u.Table, u.Filter, u.Value)), u.Key...)
}

func (u Update) SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error) {
	item, err := toMap(u.Value)
	if err != nil {
		return "", nil, tea.Error(err)
	}

	builder := squirrel.StatementBuilder.
		Update(u.Table).
		SetMap(item)

	if pp != "" {
		builder = builder.PlaceholderFormat(pp)
	}

	if u.Filter != nil {
		builder = builder.Where(u.Filter)
	}

	return builder.ToSql()
}

func (u Update) StandardMethod() StandardMethod {
	return StandardMethod{
		Update: true,
		Table:  u.Table,
		Key:    u.Key,
		Value:  u.Value,
		Filter: u.Filter,
	}
}
