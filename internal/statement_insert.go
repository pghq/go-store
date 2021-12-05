package internal

import (
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"
)

var _ Stmt = Insert{}

// Insert value
type Insert struct {
	Table string
	Key   []byte
	Value interface{}
}

func (i Insert) Bytes() []byte {
	return append([]byte(fmt.Sprintf("insert:%s:%v:", i.Table, i.Value)), i.Key...)
}

func (i Insert) SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error) {
	item, err := toMap(i.Value)
	if err != nil {
		return "", nil, tea.Error(err)
	}

	builder := squirrel.StatementBuilder.
		Insert(i.Table).
		SetMap(item)

	if pp != "" {
		builder = builder.PlaceholderFormat(pp)
	}

	return builder.ToSql()
}

func (i Insert) StandardMethod() StandardMethod {
	return StandardMethod{
		Insert: true,
		Table:  i.Table,
		Key:    i.Key,
		Value:  i.Value,
	}
}
