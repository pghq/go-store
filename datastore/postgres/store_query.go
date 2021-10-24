package postgres

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore"
)

// Query creates a query for the database.
func (s *Store) Query() datastore.Query {
	return NewQuery(s)
}

// Query is an instance of the repository query for Postgres.
type Query struct {
	store *Store
	opts  []func(builder squirrel.SelectBuilder) squirrel.SelectBuilder
}

func (q *Query) Secondary() datastore.Query {
	if q.store != nil {
		q.store.pool = q.store.secondary
	}

	return q
}

func (q *Query) From(collection string) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.From(collection)
	})

	return q
}

func (q *Query) And(collection string, args ...interface{}) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Join(collection, args...)
	})

	return q
}

func (q *Query) Filter(filter datastore.Filter) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Where(filter)
	})

	return q
}

func (q *Query) Order(by string) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.OrderBy(by)
	})

	return q
}

func (q *Query) First(first int) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Limit(uint64(first))
	})

	return q
}

func (q *Query) After(key string, value interface{}) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Where(squirrel.GtOrEq{key: value})
	})

	return q
}

func (q *Query) Return(key string, args ...interface{}) datastore.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Column(key, args...)
	})

	return q
}

func (q *Query) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select()

	for _, opt := range q.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}

func (q *Query) Execute(ctx context.Context) (datastore.Cursor, error) {
	sql, args, err := q.Statement()
	if err != nil {
		return nil, errors.BadRequest(err)
	}

	rows, err := q.store.pool.Query(ctx, sql, args...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.NoContent(err)
		}

		return nil, errors.Wrap(err)
	}

	return NewCursor(rows), nil
}

// NewQuery creates a new query for the Postgres database.
func NewQuery(store *Store) *Query {
	q := Query{
		store: store,
	}

	return &q
}
