package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Update(table string, k database.Key, v interface{}, _ ...database.CommandOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	m, err := database.Map(v)
	if err != nil {
		return tea.Stack(err)
	}
	m[k.Name] = k.Value
	stmt, args, err := squirrel.StatementBuilder.
		Update(table).
		SetMap(m).
		Where(squirrel.Eq{k.Name: k.Value}).
		PlaceholderFormat(tx.ph).
		ToSql()
	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args)
	return tx.uow.Exec(span, stmt, args...)
}
