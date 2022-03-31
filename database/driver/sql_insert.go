package driver

import (
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(table string, v interface{}) error {
	if tx.err != nil {
		return trail.Stacktrace(tx.err)
	}

	span := trail.StartSpan(tx.ctx, "database.operation")
	defer span.Finish()

	m, err := database.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		Insert(table).
		SetMap(m).
		PlaceholderFormat(tx.ph)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	span.Tag("sql.statement", stmt)
	span.Tag("sql.arguments", fmt.Sprintf("%+v", args))
	return tx.uow.Exec(span.Context(), stmt, args...)
}
