package sql

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-store/internal/encode"
	"github.com/pghq/go-store/provider"
)

type repository Provider

func (r repository) First(ctx context.Context, spec provider.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	err = r.db.GetContext(ctx, v, stmt, args...)
	return trail.Stacktrace(err)
}

func (r repository) List(ctx context.Context, spec provider.Spec, v interface{}) error {
	stmt, args, err := spec.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	return r.db.SelectContext(ctx, v, stmt, args...)
}

func (r repository) Add(ctx context.Context, collection string, v interface{}) error {
	data, err := encode.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		Insert(collection).
		SetMap(data)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.db.ExecContext(ctx, stmt, args...)
	return trail.Stacktrace(err)
}

func (r repository) Edit(ctx context.Context, collection string, spec provider.Spec, v interface{}) error {
	data, err := encode.Map(v)
	if err != nil {
		return trail.Stacktrace(err)
	}

	builder := squirrel.StatementBuilder.
		Update(collection).
		Where(spec).
		SetMap(data)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.db.ExecContext(ctx, stmt, args...)
	return trail.Stacktrace(err)
}

func (r repository) Remove(ctx context.Context, collection string, spec provider.Spec) error {
	builder := squirrel.StatementBuilder.
		Delete(collection).
		Where(spec)

	stmt, args, err := builder.ToSql()
	if err != nil {
		return trail.Stacktrace(err)
	}

	_, err = r.db.ExecContext(ctx, stmt, args...)
	return trail.Stacktrace(err)
}
