package postgres

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Update creates an update command for the database.
func (c *Client) Update() client.Update {
	return NewUpdate(c)
}

// Update is an instance of the update repository command using Postgres.
type Update struct {
	client *Client
	opts   []func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder
}

func (a *Update) In(collection string) client.Update {
	a.opts = append(a.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
		return builder.Table(collection)
	})

	return a
}

func (a *Update) Item(snapshot map[string]interface{}) client.Update {
	a.opts = append(a.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
		return builder.SetMap(snapshot)
	})

	return a
}

func (a *Update) Filter(filter client.Filter) client.Update {
	a.opts = append(a.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
		return builder.Where(filter)
	})

	return a
}

func (a *Update) Execute(ctx context.Context) (int, error) {
	sql, args, err := a.Statement()
	if err != nil {
		return 0, errors.BadRequest(err)
	}

	tag, err := a.client.pool.Exec(ctx, sql, args...)
	if err != nil {
		if IsIntegrityConstraintViolation(err) {
			return 0, errors.BadRequest(err)
		}
		return 0, errors.Wrap(err)
	}

	return int(tag.RowsAffected()), nil
}

func (a *Update) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update("")

	for _, opt := range a.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}

// NewUpdate creates a new update command for the Postgres database.
func NewUpdate(client *Client) *Update {
	a := Update{client: client}
	return &a
}
