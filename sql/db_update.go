package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Update(table string, k, v interface{}, opts ...db.CommandOption) error {
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
		Update(table).
		SetMap(m).
		Where(squirrel.Eq{keyName: k}).
		PlaceholderFormat(placeholder(cmd.SQLPlaceholder)).
		ToSql()
	if err != nil {
		return tea.NewError(err)
	}

	_, err = tx.unit.ExecContext(tx.ctx, stmt, args...)
	return err
}
