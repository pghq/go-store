package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, _, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	m, err := database.Map(v)
	if err != nil {
		return tea.Stacktrace(err)
	}

	req := database.NewRequest(args...)
	builder := squirrel.StatementBuilder.
		Insert(table).
		SetMap(m).
		PlaceholderFormat(tx.ph)

	for _, suffix := range req.Suffix {
		builder = builder.Suffix(suffix.Format, suffix.Args...)
	}

	stmt, args, err := builder.ToSql()
	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args)
	return tx.uow.Exec(span, stmt, args...)
}
