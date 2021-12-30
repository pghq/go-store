package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) Update(table string, k, v interface{}, args ...interface{}) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	m, err := database.Map(v)
	if err != nil {
		return tea.Stack(err)
	}

	args = append(args, k)
	req := database.NewRequest(args...)
	builder := squirrel.StatementBuilder.
		Update(table).
		SetMap(m).
		PlaceholderFormat(tx.ph)

	for _, eq := range req.Eq {
		builder = builder.Where(squirrel.Eq(eq))
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
