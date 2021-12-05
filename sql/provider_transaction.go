package sql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
)

// Txn creates a database transaction.
func (p *Provider) Txn(ctx context.Context, ro ...bool) (internal.Txn, error) {
	tx, err := p.client.BeginTxx(ctx, &sql.TxOptions{ReadOnly: len(ro) > 0 && ro[0] == true})
	if err != nil {
		return nil, tea.Error(err)
	}

	t := txn{ctx: ctx, tx: tx, pp: p.conf.SQLPlaceholderPrefix}
	return &t, err
}

// txn is an instance of internal.Txn for SQL.
type txn struct {
	ctx context.Context
	tx  *sqlx.Tx
	pp  internal.SQLPlaceholderPrefix
}

func (t *txn) Exec(statement internal.Stmt, dst ...interface{}) internal.Resolver {
	s, args, err := statement.SQL(t.pp)
	if err != nil {
		return internal.ExecResponse(0, tea.BadRequest(err))
	}

	m := statement.StandardMethod()
	if m.Get || m.List {
		if len(dst) == 0 {
			return internal.ExecResponse(0, tea.NewError("missing destination"))
		}

		fn := t.tx.SelectContext
		if m.Get {
			fn = t.tx.GetContext
		}

		if err := fn(t.ctx, dst[0], s, args...); err != nil {
			if tea.IsError(err, sql.ErrNoRows) {
				return internal.ExecResponse(0, tea.NoContent(err))
			}
			return internal.ExecResponse(0, tea.Error(err))
		}

		return internal.ExecResponse(1, nil)
	}

	tag, err := t.tx.Exec(s, args...)
	if err != nil {
		return internal.ExecResponse(0, tea.Error(err))
	}

	ra, err := tag.RowsAffected()
	return internal.ExecResponse(int(ra), err)
}

func (t *txn) Commit() error {
	return t.tx.Commit()

}
func (t *txn) Rollback() error {
	return t.tx.Rollback()
}
