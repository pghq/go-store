package sql

import (
	"context"
	"database/sql"
	"io/fs"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-store/provider"
	"github.com/pghq/go-store/provider/sql/internal"
)

// Provider to sql database
type Provider struct {
	db *sqlx.DB
}

func (p Provider) Repository() provider.Repository {
	return repository(p)
}

func (p Provider) Begin(ctx context.Context, opts ...provider.TxOption) (provider.UnitOfWork, error) {
	conf := provider.TxConfig{}
	for _, opt := range opts {
		opt(&conf)
	}

	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: conf.ReadOnly})
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	return UnitOfWork{tx: tx}, nil
}

// New creates a new sql database provider
func New(dialect string, dsn string, migrations fs.FS, opts ...Option) (*Provider, error) {
	p := Provider{}
	db, err := sqlx.ConnectContext(context.Background(), dialect, dsn)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	db.SetConnMaxLifetime(time.Hour)
	for _, opt := range opts {
		opt(db)
	}

	p.db = db
	if migrations != nil {
		err := internal.Apply(internal.Config{
			DB:      db.DB,
			FS:      migrations,
			Dialect: dialect,
		})

		if err != nil {
			return nil, trail.Stacktrace(err)
		}
	}

	return &p, nil
}

// Option A sql provider option
type Option func(db *sqlx.DB)

// WithMaxOpenConns configure sql with custom max open connections
func WithMaxOpenConns(n int) Option {
	return func(db *sqlx.DB) {
		db.SetMaxOpenConns(n)
	}
}

// WithMaxIdleConns configure SQL with custom max idle connections
func WithMaxIdleConns(n int) Option {
	return func(db *sqlx.DB) {
		db.SetMaxIdleConns(n)
	}
}

// WithConnMaxLifetime configure SQL with custom max connection lifetime
func WithConnMaxLifetime(d time.Duration) Option {
	return func(db *sqlx.DB) {
		db.SetConnMaxLifetime(d)
	}
}

// WithConnMaxIdleTime configure SQL with custom max connection lifetime
func WithConnMaxIdleTime(d time.Duration) Option {
	return func(db *sqlx.DB) {
		db.SetConnMaxIdleTime(d)
	}
}
