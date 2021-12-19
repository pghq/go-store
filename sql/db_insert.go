package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Insert(table string, k, v interface{}, opts ...db.CommandOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	cmd := db.CommandWith(opts)
	keyName := cmd.KeyName(v)
	m, err := db.Map(v)
	if err != nil {
		return tea.Error(err)
	}

	m[keyName] = k
	stmt, args, err := squirrel.StatementBuilder.
		Insert(table).
		SetMap(m).
		PlaceholderFormat(placeholder(cmd.SQLPlaceholder)).
		ToSql()

	if err != nil {
		return tea.NewError(err)
	}

	if _, err := tx.unit.ExecContext(tx.ctx, stmt, args...); err != nil {
		return tea.Error(err)
	}

	return nil
}
