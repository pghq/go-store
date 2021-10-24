package postgres

import (
	"context"

	"github.com/Masterminds/squirrel"
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
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.From(collection)
	})

	return q
}

func (q *Query) And(collection string, args ...interface{}) client.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Join(collection, args...)
	})

	return q
}

func (q *Query) Filter(filter client.Filter) client.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Where(filter)
	})

	return q
}

func (q *Query) Order(by string) client.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.OrderBy(by)
	})

	return q
}

func (q *Query) First(first int) client.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Limit(uint64(first))
	})

	return q
}

func (q *Query) After(key string, value interface{}) client.Query {
	q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
		return builder.Where(squirrel.GtOrEq{key: value})
	})

	return q
}

func (q *Query) Return(key string, args ...interface{}) client.Query {
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

func (q *Query) Execute(ctx context.Context) (client.Cursor, error) {
	sql, args, err := q.Statement()
	if err != nil {
		return nil, errors.BadRequest(err)
	}

	rows, err := q.client.pool.Query(ctx, sql, args...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.NoContent(err)
		}

		return nil, errors.Wrap(err)
	}

	return NewCursor(rows), nil
}

// NewQuery creates a new query for the Postgres database.
func NewQuery(client *Client) *Query {
	q := Query{
		client: client,
	}

	return &q
}
