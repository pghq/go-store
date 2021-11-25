package ark

import (
	"time"

	"github.com/Masterminds/squirrel"

	"github.com/pghq/go-ark/client"
)

// Remove creates a remove command for the database.
func (c *pgClient) Remove() client.Remove {
	return &pgRemove{client: c}
}

// pgRemove is an instance of the repository remove command for Postgres.
type pgRemove struct {
	client *pgClient
	opts   []func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder
}

func (r *pgRemove) From(collection string) client.Remove {
	if collection != "" {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.From(collection)
		})
	}

	return r
}

func (r *pgRemove) Filter(filter interface{}) client.Remove {
	if f, ok := filter.(PgCond); ok && len(f.opts) > 0 {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.Where(filter)
		})
	}

	return r
}

func (r *pgRemove) Order(by string) client.Remove {
	if by != "" {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.OrderBy(by)
		})
	}

	return r
}

func (r *pgRemove) After(key string, value *time.Time) client.Remove {
	if key != "" && value != nil && !value.IsZero() {
		r.opts = append(r.opts, func(builder squirrel.DeleteBuilder) squirrel.DeleteBuilder {
			return builder.Where(squirrel.Gt{key: value})
		})
	}

	return r
}

func (r *pgRemove) Statement() (string, []interface{}, error) {
	builder := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Delete("")

	for _, opt := range r.opts {
		builder = opt(builder)
	}

	return builder.ToSql()
}
