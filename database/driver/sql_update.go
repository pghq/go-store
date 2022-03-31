package driver

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Update(ctx context.Context, table string, query database.Query, v interface{}) error {
	if tx.err != nil {
		return trail.Stacktrace(tx.err)
	}

	span := trail.StartSpan(ctx, "database.operation")
	defer span.Finish()

	m, err := database.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		Update(table).
		SetMap(m).
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
		builder = builder.Where(squirrel.Expr(expr.Format, expr.Args...))
	}

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	span.Fields.Set("sql.statement", stmt)
	span.Fields.Set("sql.arguments", args)
	return tx.uow.Exec(span.Context(), stmt, args...)
}
