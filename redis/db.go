package redis

import (
	"context"
	"net/url"

	"github.com/go-redis/redis/v8"
	"github.com/pghq/go-tea"
)

// DB Redis database
type DB struct {
	backend *redis.Client
	err     error
}

func (d DB) Ping(ctx context.Context) error {
	if d.err != nil {
		return tea.Stacktrace(d.err)
	}

	return d.backend.Ping(ctx).Err()
}

// NewDB Create a new Redis database
func NewDB(databaseURL *url.URL) *DB {
	db := DB{}
	opts, err := redis.ParseURL(databaseURL.String())
	if err != nil {
		db.err = tea.Stacktrace(err)
		return &db
	}
	db.backend = redis.NewClient(opts)
	return &db
}
