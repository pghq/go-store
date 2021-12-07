package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pghq/go-tea"
)

// Provider is a low-level provider for the Redis.
type Provider struct {
	dsn    string
	conf   Config
	client *redis.Client
}

// NewProvider creates a new Redis provider
func NewProvider(source interface{}, conf Config) *Provider {
	dsn, _ := source.(string)
	return &Provider{
		dsn:  dsn,
		conf: conf,
	}
}

func (p *Provider) Connect(ctx context.Context) error {
	c := redis.NewClient(&redis.Options{
		Addr:         p.dsn,
		Network:      p.conf.Network,
		Username:     p.conf.Username,
		Password:     p.conf.Password,
		DB:           p.conf.DB,
		MaxRetries:   p.conf.MaxRetries,
		DialTimeout:  p.conf.DialTimeout,
		ReadTimeout:  p.conf.ReadTimeout,
		WriteTimeout: p.conf.WriteTimeout,
		PoolSize:     p.conf.PoolSize,
		MinIdleConns: p.conf.MinIdleConns,
		MaxConnAge:   p.conf.MaxConnAge,
		PoolTimeout:  p.conf.PoolTimeout,
		IdleTimeout:  p.conf.IdleTimeout,
	})

	if err := c.Ping(ctx).Err(); err != nil {
		return tea.Error(err)
	}

	p.client = c
	return nil
}

// Config is a Redis provider configuration.
type Config struct {
	Network      string
	Username     string
	Password     string
	DB           int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
	MaxConnAge   time.Duration
	PoolTimeout  time.Duration
	IdleTimeout  time.Duration
}
