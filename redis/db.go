package redis

import (
	"context"

	"github.com/go-redis/redis/v8"

	"github.com/pghq/go-ark/db"
)

// DB | Redis database
type DB struct {
	backend *redis.Client
}

func (d DB) Ping(ctx context.Context) error {
	return d.backend.Ping(ctx).Err()
}

// NewDB | Create a new Redis database
func NewDB(opts ...db.Option) *DB {
	config := db.ConfigWith(opts)
	config.RedisOptions.Addr = config.DSN
	return &DB{backend: redis.NewClient(&config.RedisOptions)}
}
