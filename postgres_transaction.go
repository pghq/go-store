package ark

import (
	"context"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/client"
)

// Transaction creates a database transaction for Postgres.
func (c *pgClient) Transaction(ctx context.Context, ro bool) (client.Transaction, error) {
	selected := c.pools.primary
	if ro {
		selected = c.pools.secondary
	}

	tx, err := selected.Begin(ctx)
	if err != nil {
		return nil, tea.Error(err)
	}

	t := pgTransaction{ctx: ctx, tx: tx}
	return &t, err
}

// pgTransaction is an instance of Transaction for pg
type pgTransaction struct {
	ctx context.Context
	tx  pgx.Tx
}

func (t *pgTransaction) Execute(statement client.Encoder, dst ...interface{}) (int, error) {
	sql, args, err := statement.Statement()
	if err != nil {
		return 0, tea.BadRequest(err)
	}

	if len(dst) > 0 {
		if err := pgxscan.Select(t.ctx, t.tx, dst[0], sql, args...); err != nil {
			if tea.IsError(err, pgx.ErrNoRows) {
				return 0, tea.NoContent(err)
			}
			return 0, tea.Error(err)
		}

		return 0, nil
	}

	tag, err := t.tx.Exec(t.ctx, sql, args...)
	if err != nil {
		if IsPgIntegrityConstraintViolation(err) {
			return 0, tea.BadRequest(err)
		}
		return 0, tea.Error(err)
	}

	return int(tag.RowsAffected()), nil
}

func (t *pgTransaction) Commit() error {
	if err := t.tx.Commit(t.ctx); err != nil {
		return tea.Error(err)
	}

	return nil

}
func (t *pgTransaction) Rollback() error {
	if err := t.tx.Rollback(t.ctx); err != nil {
		return tea.Error(err)
	}

	return nil
}
