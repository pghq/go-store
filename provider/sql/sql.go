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
	db        *sqlx.DB
	migration fs.FS
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
func New(dialect string, dsn string, opts ...Option) (*Provider, error) {
	p := Provider{}
	for _, opt := range opts {
		opt(&p)
	}

	db, err := sqlx.ConnectContext(context.Background(), dialect, dsn)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	// todo: allow consumer to configure
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	db.SetConnMaxLifetime(time.Hour)

	p.db = db
	if p.migration != nil {
		err := internal.Apply(internal.Config{
			DB:      db.DB,
			FS:      p.migration,
			Dialect: dialect,
		})

		if err != nil {
			return nil, trail.Stacktrace(err)
		}
	}

	return &p, nil
}

// Option A sql provider option
type Option func(*Provider)

// WithMigration Use database migration
func WithMigration(fs fs.ReadDirFS) Option {
	return func(provider *Provider) {
		provider.migration = fs
	}
}
