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

func (u *Update) In(collection string) client.Update {
	if collection != "" {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.Table(collection)
		})
	}

	return u
}

func (u *Update) Item(value map[string]interface{}) client.Update {
	if value != nil {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.SetMap(value)
		})
	}

	return u
}

func (u *Update) Filter(filter interface{}) client.Update {
	if f, ok := filter.(Cond); ok && len(f.opts) > 0 {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.Where(filter)
		})
	}

	return u
}

func (u *Update) Execute(ctx context.Context) (int, error) {
	sql, args, err := u.Statement()
	if err != nil {
		return 0, errors.BadRequest(err)
	}

	tag, err := u.client.pool.Exec(ctx, sql, args...)
	if err != nil {
		if IsIntegrityConstraintViolation(err) {
			return 0, errors.BadRequest(err)
		}
		return 0, errors.Wrap(err)
	}

	return int(tag.RowsAffected()), nil
}

func (u *Update) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update("")

	for _, opt := range u.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}

// NewUpdate creates a new update command for the Postgres database.
func NewUpdate(client *Client) *Update {
	a := Update{client: client}
	return &a
}
