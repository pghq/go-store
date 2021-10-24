package postgres

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore"
)

// Remove creates a remove command for the database.
func (s *Store) Remove() datastore.Remove {
	return NewRemove(s)
}

// Remove is an instance of the repository remove command for Postgres.
type Remove struct {
	store *Store
	opts  []func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder
}

func (r *Remove) From(collection string) datastore.Remove {
	r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
		return builder.From(collection)
	})

	return r
}

func (r *Remove) Filter(filter datastore.Filter) datastore.Remove {
	r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
		return builder.Where(filter)
	})

	return r
}

func (r *Remove) Order(by string) datastore.Remove {
	r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
		return builder.OrderBy(by)
	})

	return r
}

func (r *Remove) First(first int) datastore.Remove {
	r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
		return builder.Limit(uint64(first))
	})

	return r
}

func (r *Remove) After(key string, value interface{}) datastore.Remove {
	r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
		return builder.Where(squirrel.GtOrEq{key: value})
	})

	return r
}

func (r *Remove) Execute(ctx context.Context) (int, error) {
	sql, args, err := r.Statement()
	if err != nil {
		return 0, errors.BadRequest(err)
	}

	tag, err := r.store.pool.Exec(ctx, sql, args...)
	if err != nil {
		if IsIntegrityConstraintViolation(err) {
			return 0, errors.BadRequest(err)
		}
		return 0, errors.Wrap(err)
	}

	return int(tag.RowsAffected()), nil
}

func (r *Remove) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Delete("")

	for _, opt := range r.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}

// NewRemove creates a remove command for the Postgres database.
func NewRemove(store *Store) *Remove {
	r := Remove{store: store}
	return &r
}
