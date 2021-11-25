package ark

import (
	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-ark/client"
)

// Update creates an update command for the database.
func (c *pgClient) Update() client.Update {
	return &pgUpdate{client: c}
}

// pgUpdate is an instance of the update repository command using Postgres.
type pgUpdate struct {
	client *pgClient
	opts   []func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder
}

func (u *pgUpdate) In(collection string) client.Update {
	if collection != "" {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.Table(collection)
		})
	}

	return u
}

func (u *pgUpdate) Item(value map[string]interface{}) client.Update {
	if value != nil {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.SetMap(value)
		})
	}

	return u
}

func (u *pgUpdate) Filter(filter interface{}) client.Update {
	if f, ok := filter.(PgCond); ok && len(f.opts) > 0 {
		u.opts = append(u.opts, func(builder squirrel.UpdateBuilder) squirrel.UpdateBuilder {
			return builder.Where(filter)
		})
	}

	return u
}

func (u *pgUpdate) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update("")

	for _, opt := range u.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}
