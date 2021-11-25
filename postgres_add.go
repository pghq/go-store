package ark

import (
	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-ark/client"
)

// Add creates an add command for the database.
func (c *pgClient) Add() client.Add {
	return &pgAdd{client: c}
}

// pgAdd is an instance of the add repository command using Postgres.
type pgAdd struct {
	client *pgClient
	opts   []func(builder squirrel.InsertBuilder) squirrel.InsertBuilder
}

func (a *pgAdd) To(collection string) client.Add {
	a.opts = append(a.opts, func(builder squirrel.InsertBuilder) squirrel.InsertBuilder {
		return builder.Into(collection)
	})

	return a
}

func (a *pgAdd) Item(value map[string]interface{}) client.Add {
	if value != nil {
		a.opts = append(a.opts, func(builder squirrel.InsertBuilder) squirrel.InsertBuilder {
			return builder.SetMap(value)
		})
	}

	return a
}

func (a *pgAdd) Query(q client.Query) client.Add {
	if q, ok := q.(*pgQuery); ok {
		s := squirrel.StatementBuilder.
			PlaceholderFormat(squirrel.Dollar).
			Select()
		for _, opt := range q.opts {
			s = opt(s)
		}

		a.opts = append(a.opts, func(builder squirrel.InsertBuilder) squirrel.InsertBuilder {
			return builder.Select(s)
		})
	}

	return a
}

func (a *pgAdd) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert("")

	for _, opt := range a.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}
