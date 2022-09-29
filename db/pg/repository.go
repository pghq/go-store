package pg

import (
	"context"

	"github.com/pghq/go-store/db"
	"github.com/pghq/go-store/db/pg/internal"
	"github.com/pghq/go-store/enc"
	"github.com/pghq/go-tea/trail"

	"github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

var (
	// ErrNotFound is returned for get ops with no results
	ErrNotFound = trail.NewErrorNotFound("the requested item does not exist")

	// ErrUnique is return for write ops that violate unique constraint
	ErrUnique = trail.NewErrorConflict("an item already exists matching your request")

	// ErrBadValue is returned for encoding errors
	ErrBadValue = trail.NewError("bad value")
)

type repository Provider

func (r repository) Batch(ctx context.Context, batch db.Batch) error {
	queue := pgx.Batch{}
	for _, item := range batch {
		if !item.Skip {
			sql, args, err := item.Spec.ToSql()
			if err != nil {
				return trail.Stacktrace(err)
			}
			queue.Queue(sql, args...)
		}
	}

	res := r.conn(ctx).SendBatch(ctx, &queue)
	defer res.Close()

	for _, item := range batch {
		if !item.Skip {
			var handler func() error
			if item.Value == nil {
				handler = func() error {
					_, err := res.Exec()
					return err
				}
			} else if item.One {
				handler = func() error {
					return pgxscan.Get(ctx, batchResults{res}, item.Value, "")
				}
			} else {
				handler = func() error {
					return pgxscan.Select(ctx, batchResults{res}, item.Value, "")
				}
			}

			if err := handler(); err != nil {
				err = pgError(err)
				if !item.Optional || trail.IsFatal(err) {
					return trail.Stacktrace(err)
				}
			}
		}
	}

	return nil
}

func (r repository) One(ctx context.Context, spec db.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	return trail.Stacktrace(pgError(pgxscan.Get(ctx, r.conn(ctx), v, stmt, args...)))
}

func (r repository) All(ctx context.Context, spec db.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	return trail.Stacktrace(pgError(pgxscan.Select(ctx, r.conn(ctx), v, stmt, args...)))
}

func (r repository) Add(ctx context.Context, collection string, v interface{}) error {
	data := enc.Map(v)
	if data == nil {
		return ErrBadValue
	}

	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert(collection).
		SetMap(data)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.conn(ctx).Exec(ctx, stmt, args...)
	return trail.Stacktrace(pgError(err))
}

func (r repository) Edit(ctx context.Context, collection string, spec db.Spec, v interface{}) error {
	data := enc.Map(v)
	if data == nil {
		return ErrBadValue
	}

	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update(collection).
		Where(spec).
		SetMap(data)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.conn(ctx).Exec(ctx, stmt, args...)
	return trail.Stacktrace(pgError(err))
}

func (r repository) Remove(ctx context.Context, collection string, spec db.Spec) error {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Delete(collection).
		Where(spec)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.conn(ctx).Exec(ctx, stmt, args...)
	return trail.Stacktrace(pgError(err))
}

type batchResults struct {
	pgx.BatchResults
}

func (b batchResults) Query(_ context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	return b.BatchResults.Query()
}

func (r repository) conn(ctx context.Context) conn {
	uow, ok := db.UnitOfWorkValue(ctx)
	if ok {
		if tx, ok := uow.Tx().(pgx.Tx); ok {
			return tx
		}
	}

	return r.db
}

type conn interface {
	pgxscan.Querier
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

func pgError(err error) error {
	switch {
	case trail.IsError(err, pgx.ErrNoRows):
		return ErrNotFound
	case internal.IsErrorCode(err, internal.ErrCodeUniqueViolation):
		return ErrUnique
	}
	return err
}
