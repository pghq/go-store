package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) Remove(table string, k interface{}, opts ...db.CommandOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	cmd := db.CommandWith(opts)
	keyName := cmd.KeyName(nil)
	stmt, args, err := squirrel.StatementBuilder.
		Delete(table).
		Where(squirrel.Eq{keyName: k}).
		PlaceholderFormat(placeholder(cmd.SQLPlaceholder)).
		ToSql()
	if err != nil {
		return tea.NewError(err)
	}

	_, err = tx.unit.ExecContext(tx.ctx, stmt, args...)
	return err
}
