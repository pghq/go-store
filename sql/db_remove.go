package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Remove(table string, k database.Key, _ ...database.CommandOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	stmt, args, err := squirrel.StatementBuilder.
		Delete(table).
		Where(squirrel.Eq{k.Name: k.Value}).
		PlaceholderFormat(tx.ph).
		ToSql()
	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args...)
	_, err = tx.unit.ExecContext(span, stmt, args...)
	return err
}
