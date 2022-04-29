package driver

import (
	"context"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Get(ctx context.Context, table string, query database.Query, v interface{}) error {
	if tx.err != nil {
		return trail.Stacktrace(tx.err)
	}

	span := trail.StartSpan(ctx, "SQL.Get")
	defer span.Finish()

	query.Format = tx.ph
	query.Limit = 1
	query.Table = table
	stmt, args, err := query.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	span.Tags.Set("Statement", stmt)
	span.Tags.SetJSON("Arguments", args)
	return tx.uow.Get(span.Context(), v, stmt, args...)
}
