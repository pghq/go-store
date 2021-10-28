package postgres

import (
	"context"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Query creates a query for the database.
func (c *Client) Query() client.Query {
	return NewQuery(c)
}

// Query is an instance of the repository query for Postgres.
type Query struct {
	client *Client
	opts   []func(builder squirrel.SelectBuilder) squirrel.SelectBuilder
}

func (q *Query) Secondary() client.Query {
	if q.client != nil {
		q.client.pool = q.client.secondary
	}

	return q
}

func (q *Query) From(collection string) client.Query {
	if collection != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.From(collection)
		})
	}

	return q
}

func (q *Query) And(collection string, args ...interface{}) client.Query {
	if collection != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Join(collection, args...)
		})
	}

	return q
}

func (q *Query) Filter(filter client.Filter) client.Query {
	if filter != nil {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Where(filter)
		})
	}

	return q
}

func (q *Query) Order(by string) client.Query {
	if by != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.OrderBy(by)
		})
	}

	return q
}

func (q *Query) First(first int) client.Query {
	if first > 0 {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Limit(uint64(first))
		})
	}

	return q
}

func (q *Query) After(key string, value *time.Time) client.Query {
	if key != "" && value != nil && !value.IsZero() {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Where(squirrel.GtOrEq{key: value})
		})
	}

	return q
}

func (q *Query) Return(key string, args ...interface{}) client.Query {
	if key != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Column(key, args...)
		})
	}

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

func (q *Query) Execute(ctx context.Context, dst interface{}) error {
	sql, args, err := q.Statement()
	if err != nil {
		return errors.BadRequest(err)
	}

	if err := pgxscan.Select(ctx, q.client.pool, dst, sql, args...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NoContent(err)
		}

		return errors.Wrap(err)
	}

	return nil
}

// NewQuery creates a new query for the Postgres database.
func NewQuery(client *Client) *Query {
	q := Query{
		client: client,
	}

	return &q
}
