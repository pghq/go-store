package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Get(table string, k database.Key, v interface{}, opts ...database.QueryOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	query := database.QueryWith(opts)
	stmt, args, err := squirrel.StatementBuilder.
		Select().
		From(table).
		Limit(1).
		Columns(query.Fields...).
		PlaceholderFormat(tx.ph).
		Where(squirrel.Eq{k.Name: k.Value}).
		ToSql()
	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args...)
	return tx.uow.Get(span, v, stmt, args...)
}
