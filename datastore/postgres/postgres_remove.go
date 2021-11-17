package postgres

import (
	"time"

	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-datastore/datastore/client"
)

// Remove creates a remove command for the database.
func (c *Client) Remove() client.Remove {
	return NewRemove(c)
}

// Remove is an instance of the repository remove command for Postgres.
type Remove struct {
	client *Client
	opts   []func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder
}

func (r *Remove) From(collection string) client.Remove {
	if collection != "" {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.From(collection)
		})
	}

	return r
}

func (r *Remove) Filter(filter interface{}) client.Remove {
	if f, ok := filter.(Cond); ok && len(f.opts) > 0 {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.Where(filter)
		})
	}

	return r
}

func (r *Remove) Order(by string) client.Remove {
	if by != "" {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.OrderBy(by)
		})
	}

	return r
}

func (r *Remove) After(key string, value *time.Time) client.Remove {
	if key != "" && value != nil && !value.IsZero() {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.Where(squirrel.Gt{key: value})
		})
	}

	return r
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
func NewRemove(client *Client) *Remove {
	r := Remove{client: client}
	return &r
}
