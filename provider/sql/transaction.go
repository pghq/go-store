package sql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// UnitOfWork light wrapper around a pgx transaction (ignoring rollback errors)
type UnitOfWork struct {
	tx *sqlx.Tx
}

func (u UnitOfWork) Commit(_ context.Context) error {
	return u.tx.Commit()
}

func (u UnitOfWork) Rollback(_ context.Context) {
	_ = u.tx.Rollback()
}
