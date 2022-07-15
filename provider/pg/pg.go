package pg

import (
	"context"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-store/provider"
	"github.com/pghq/go-store/provider/pg/internal"
)

// Provider to sql database
type Provider struct {
	db *pgxpool.Pool
}

func (p Provider) Repository() provider.Repository {
	return repository(p)
}

func (p Provider) Begin(ctx context.Context, opts ...provider.TxOption) (provider.UnitOfWork, error) {
	conf := provider.TxConfig{}
	for _, opt := range opts {
		opt(&conf)
	}

	pgxOpts := pgx.TxOptions{}
	if conf.ReadOnly {
		pgxOpts.AccessMode = pgx.ReadOnly
	}

	tx, err := p.db.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	return unitOfWork{tx: tx}, nil
}

// New creates a new pg database provider
func New(dsn string, migrations fs.FS, opts ...Option) (*Provider, error) {
	conf := ProviderConfig{
		MaxConns:        100,
		MaxConnLifetime: time.Hour,
		ConnectTimeout:  30 * time.Second,
	}

	for _, opt := range opts {
		opt(&conf)
	}

	pgxConf, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	pgxConf.MaxConns = conf.MaxConns
	pgxConf.MaxConnLifetime = conf.MaxConnLifetime

	ctx, cancel := context.WithTimeout(context.Background(), conf.ConnectTimeout)
	defer cancel()

	db, err := pgxpool.ConnectConfig(ctx, pgxConf)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	if err := internal.Apply(stdlib.OpenDB(*pgxConf.ConnConfig), migrations); err != nil {
		return nil, trail.Stacktrace(err)
	}

	p := Provider{db: db}
	return &p, nil
}

// ProviderConfig custom options for pg configuration
type ProviderConfig struct {
	MaxConns        int32
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

// Option A sql provider option
type Option func(conf *ProviderConfig)

// WithMaxConns configure pg with custom max connections
func WithMaxConns(n int32) Option {
	return func(conf *ProviderConfig) {
		conf.MaxConns = n
	}
}

// WithMaxConnLifetime configure pg with custom max connection lifetime
func WithMaxConnLifetime(d time.Duration) Option {
	return func(conf *ProviderConfig) {
		conf.MaxConnLifetime = d
	}
}

// WithConnectTimeout configure pg with custom connect timeout
func WithConnectTimeout(d time.Duration) Option {
	return func(conf *ProviderConfig) {
		conf.ConnectTimeout = d
	}
}

type unitOfWork struct {
	tx pgx.Tx
}

func (u unitOfWork) Commit(ctx context.Context) error {
	return u.tx.Commit(ctx)
}

func (u unitOfWork) Rollback(ctx context.Context) {
	_ = u.tx.Rollback(ctx)
}
