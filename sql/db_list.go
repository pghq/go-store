package sql

import (
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) List(table string, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stacktrace(tx.err)
	}

	req := database.NewRequest(args...)
	builder := squirrel.StatementBuilder.
		Select().
		From(table).
		Limit(uint64(req.Limit)).
		Offset(uint64(req.Page)).
		OrderBy(req.OrderBy...).
		GroupBy(req.GroupBy...).
		PlaceholderFormat(tx.ph)

	for key, value := range req.Fields {
		column := interface{}(squirrel.Alias(squirrel.Expr(value.Format, value.Args...), key))
		if key == value.Format {
			if !strings.Contains(key, ".") {
				key = table + "." + key
			}
			column = key
		}
		builder = builder.Column(column)
	}

	for _, expr := range req.Tables {
		builder = builder.JoinClause(expr.Format, expr.Args...)
	}

	for _, eq := range req.Eq {
		builder = builder.Where(squirrel.Eq(eq))
	}

	for k, v := range req.Px {
		builder = builder.Where(squirrel.ILike(map[string]interface{}{k: fmt.Sprintf("%s%%", v)}))
	}

	for _, neq := range req.NotEq {
		builder = builder.Where(squirrel.NotEq(neq))
	}

	for _, lt := range req.Lt {
		builder = builder.Where(squirrel.Lt(lt))
	}

	for _, gt := range req.Gt {
		builder = builder.Where(squirrel.Gt(gt))
	}

	for _, xeq := range req.XEq {
		builder = builder.Where(squirrel.Like(xeq))
	}

	for _, nxeq := range req.XEq {
		builder = builder.Where(squirrel.NotLike(nxeq))
	}

	for _, expr := range req.Filters {
		builder = builder.Where(squirrel.Expr(expr.Format, expr.Args...))
	}

	stmt, args, err := builder.
		ToSql()
	if err != nil {
		return tea.Err(err)
	}

	span := tea.Nest(tx.ctx, "sql")
	defer span.End()
	span.Tag("statement", stmt)
	span.Tag("arguments", args)
	if err := tx.uow.List(span, v, stmt, args...); err != nil {
		return tea.Stacktrace(err)
	}

	return nil
}
