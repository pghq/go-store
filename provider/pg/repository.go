package pg

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-store/internal/encode"
	"github.com/pghq/go-store/provider"
	"github.com/pghq/go-store/provider/pg/internal"
)

var (
	// ErrNotFound is returned for get ops with no results
	ErrNotFound = trail.NewErrorNotFound("the requested item does not exist")

	// ErrUnique is return for write ops that violate unique constraint
	ErrUnique = trail.NewErrorConflict("an item already exists matching your request")
)

type repository Provider

func (r repository) BatchQuery(ctx context.Context, query provider.BatchQuery) error {
	queue := pgx.Batch{}
	for _, item := range query {
		if !item.Skip {
			sql, args, err := item.Spec.ToSql()
			if err != nil {
				return trail.Stacktrace(err)
			}
			queue.Queue(sql, args...)
		}
	}

	res := r.db.SendBatch(ctx, &queue)
	defer res.Close()

	for _, item := range query {
		if !item.Skip {
			handler := pgxscan.Select
			if item.One {
				handler = pgxscan.Get
			}

			if err := handler(ctx, batchResults{res}, item.Value, ""); err != nil {
				if trail.IsError(err, pgx.ErrNoRows) {
					err = ErrNotFound
				}

				if !item.Optional || trail.IsFatal(err) {
					return trail.Stacktrace(err)
				}
			}
		}
	}

	return nil
}

func (r repository) One(ctx context.Context, spec provider.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	if err = pgxscan.Get(ctx, r.db, v, stmt, args...); trail.IsError(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}

	return trail.Stacktrace(err)
}

func (r repository) All(ctx context.Context, spec provider.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	return pgxscan.Select(ctx, r.db, v, stmt, args...)
}

func (r repository) Add(ctx context.Context, collection string, v interface{}) error {
	data, err := encode.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert(collection).
		SetMap(data)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	if _, err = r.db.Exec(ctx, stmt, args...); internal.IsErrorCode(err, internal.ErrCodeUniqueViolation) {
		err = ErrUnique
	}

	return trail.Stacktrace(err)
}

func (r repository) Edit(ctx context.Context, collection string, spec provider.Spec, v interface{}) error {
	data, err := encode.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
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

	if _, err = r.db.Exec(ctx, stmt, args...); internal.IsErrorCode(err, internal.ErrCodeUniqueViolation) {
		err = ErrUnique
	}

	return trail.Stacktrace(err)
}

func (r repository) Remove(ctx context.Context, collection string, spec provider.Spec) error {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Delete(collection).
		Where(spec)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.db.Exec(ctx, stmt, args...)
	return trail.Stacktrace(err)
}

type batchResults struct {
	pgx.BatchResults
}

func (b batchResults) Query(_ context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	return b.BatchResults.Query()
}
