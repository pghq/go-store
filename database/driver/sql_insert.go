package driver

import (
	"context"
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Insert(ctx context.Context, table string, v interface{}) error {
	if tx.err != nil {
		return trail.Stacktrace(tx.err)
	}

	span := trail.StartSpan(ctx, "transaction.insert")
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

	span.Fields.Set("sql.statement", stmt)
	span.Fields.Set("sql.arguments", args)
	return tx.uow.Exec(span.Context(), stmt, args...)
}
