package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) List(table string, v interface{}, opts ...database.QueryOption) error {
	if tx.err != nil {
		return tea.Stack(tx.err)
	}

	query := database.QueryWith(opts)
	builder := squirrel.StatementBuilder.
		Select().
		From(table).
		Limit(uint64(query.Limit)).
		Offset(uint64(query.Page)).
		OrderBy(query.OrderBy...).
		PlaceholderFormat(tx.ph)

	for _, field := range query.Fields {
		builder = builder.Column(field())
	}

	for _, expr := range query.Tables {
		builder = builder.JoinClause(expr.Format, expr.Args...)
	}

	for _, eq := range query.Eq {
		builder = builder.Where(squirrel.Eq(eq))
	}

	for _, neq := range query.NotEq {
		builder = builder.Where(squirrel.NotEq(neq))
	}

	for _, lt := range query.Lt {
		builder = builder.Where(squirrel.Lt(lt))
	}

	for _, gt := range query.Gt {
		builder = builder.Where(squirrel.Gt(gt))
	}

	for _, xeq := range query.XEq {
		builder = builder.Where(squirrel.Like(xeq))
	}

	for _, nxeq := range query.XEq {
		builder = builder.Where(squirrel.NotLike(nxeq))
	}

	for _, expr := range query.Filters {
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
	span.Tag("arguments", args...)
	if err := tx.uow.List(span, v, stmt, args...); err != nil {
		return tea.Stack(err)
	}

	return nil
}
