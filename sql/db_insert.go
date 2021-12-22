package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, k database.Key, v interface{}, _ ...database.CommandOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	m, err := database.Map(v)
	if err != nil {
		return tea.Stack(err)
	}

	m[k.Name] = k.Value
	stmt, args, err := squirrel.StatementBuilder.
		Insert(table).
		SetMap(m).
		PlaceholderFormat(tx.ph).
		ToSql()

	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args...)
	if _, err := tx.unit.ExecContext(span, stmt, args...); err != nil {
		return tea.Stack(err)
	}

	return nil
}
