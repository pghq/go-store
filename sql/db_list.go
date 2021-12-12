package sql

import (
	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) List(table string, v interface{}, opts ...db.QueryOption) error {
	if tx.err != nil {
		return tea.Error(tx.err)
	}

	query := db.QueryWith(opts)
	builder := squirrel.StatementBuilder.
		Select().
		From(table).
		Limit(uint64(query.Limit)).
		Offset(uint64(query.Page)).
		Columns(query.Fields...).
		OrderBy(query.OrderBy...).
		PlaceholderFormat(placeholder(query.SQLPlaceholder))

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
		builder = builder.Where(squirrel.ILike(xeq))
	}

	for _, nxeq := range query.XEq {
		builder = builder.Where(squirrel.NotILike(nxeq))
	}

	for _, expr := range query.Expressions {
		builder = builder.Where(squirrel.Expr(expr.Format, expr.Args...))
	}

	stmt, args, err := builder.
		ToSql()
	if err != nil {
		return tea.NewError(err)
	}

	if err := tx.unit.SelectContext(tx.ctx, v, stmt, args...); err != nil {
		return tea.Error(err)
	}

	return nil
}
