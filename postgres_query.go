package ark

import (
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-ark/client"
	"github.com/pghq/go-ark/internal"
)

// Query creates a query for the database.
func (c *pgClient) Query() client.Query {
	return &pgQuery{client: c, selected: c.pools.primary}
}

// pgQuery is an instance of the repository query for Postgres.
type pgQuery struct {
	client    *pgClient
	selected  pgPool
	opts      []func(builder squirrel.SelectBuilder) squirrel.SelectBuilder
	fields    []string
	transform func(string) string
}

func (q *pgQuery) String() string {
	s, args, _ := q.Statement()
	return fmt.Sprint(s, args)
}

func (q *pgQuery) From(collection string) client.Query {
	if collection != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.From(collection)
		})
	}

	return q
}

func (q *pgQuery) Complement(collection string, args ...interface{}) client.Query {
	if collection != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.LeftJoin(collection, args...)
		})
	}

	return q
}

func (q *pgQuery) Filter(filter interface{}) client.Query {
	if f, ok := filter.(PgCond); ok && len(f.opts) > 0 {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Where(filter)
		})
	}

	return q
}

func (q *pgQuery) Order(by string) client.Query {
	if by != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.OrderBy(by)
		})
	}

	return q
}

func (q *pgQuery) First(first int) client.Query {
	if first > 0 {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Limit(uint64(first))
		})
	}

	return q
}

func (q *pgQuery) After(key string, value *time.Time) client.Query {
	if key != "" && value != nil && !value.IsZero() {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Where(squirrel.Gt{key: value})
		})
	}

	return q
}

func (q *pgQuery) Fields(fields ...interface{}) client.Query {
	q.fields = Fields(fields...)

	return q
}

func (q *pgQuery) Field(key string, args ...interface{}) client.Query {
	if key != "" {
		q.opts = append(q.opts, func(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
			return builder.Column(internal.ToSnakeCase(key), args...)
		})
	}

	return q
}

func (q *pgQuery) Transform(transform func(string) string) client.Query {
	q.transform = transform

	return q
}

func (q *pgQuery) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select()

	for _, opt := range q.opts {
		builder = opt(builder)
	}

	for _, field := range q.fields {
		field = internal.ToSnakeCase(field)
		if q.transform != nil {
			field = q.transform(field)
		}

		builder = builder.Column(field)
	}

	return builder.ToSql()
}
