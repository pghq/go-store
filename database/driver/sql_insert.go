package driver

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, v interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	m, err := database.Map(v)
	if err != nil {
		return tea.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		Insert(table).
		SetMap(m).
		PlaceholderFormat(tx.ph)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return tea.Err(err)
	}

	return tx.uow.Exec(tx.ctx, stmt, args...)
}
