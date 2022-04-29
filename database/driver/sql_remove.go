package driver

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Remove(ctx context.Context, table string, query database.Query) error {
	if tx.err != nil {
		return trail.Stacktrace(tx.err)
	}

	span := trail.StartSpan(ctx, "SQL.Remove")
	defer span.Finish()

	builder := squirrel.StatementBuilder.
		Delete(table).
		Where(squirrel.Eq(query.Eq)).
		Where(squirrel.NotEq(query.NotEq)).
		Where(squirrel.Lt(query.Lt)).
		Where(squirrel.Gt(query.Gt)).
		Where(squirrel.Like(query.XEq)).
		Where(squirrel.NotLike(query.NotXEq)).
		PlaceholderFormat(tx.ph)

	for k, v := range query.Px {
		builder = builder.Where(squirrel.ILike(map[string]interface{}{k: fmt.Sprintf("%s%%", v)}))
	}

	for _, expr := range query.Filters {
		builder = builder.Where(expr)
	}

	for _, expr := range query.Suffixes {
		builder = builder.SuffixExpr(expr)
	}

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	span.Tags.Set("Statement", stmt)
	span.Tags.SetJSON("Arguments", args)
	return tx.uow.Exec(span.Context(), stmt, args...)
}
