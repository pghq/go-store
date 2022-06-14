package provider

import (
	"context"
)

// Provider provides instances of transactions and repositories.
type Provider interface {
	Repository() Repository
	Begin(ctx context.Context, opts ...TxOption) (UnitOfWork, error)
}

// UnitOfWork to do
type UnitOfWork interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context)
}

// Repository abstraction for a collection of objects.
type Repository interface {
	First(ctx context.Context, spec Spec, v interface{}) error
	List(ctx context.Context, spec Spec, v interface{}) error
	Add(ctx context.Context, collection string, v interface{}) error
	Edit(ctx context.Context, spec Spec, v interface{}) error
	Remove(ctx context.Context, spec Spec) error
}

// Spec for querying objects
type Spec interface {
	Id() interface{}
	Collection() string
	ToSql() (string, []interface{}, error)
}

// TxConfig a configuration for transactions
type TxConfig struct {
	ReadOnly bool
}

// TxOption a configuration option for transactions
type TxOption func(conf *TxConfig)

// WithReadOnly use read-only transaction
func WithReadOnly(flag bool) TxOption {
	return func(conf *TxConfig) {
		conf.ReadOnly = flag
	}
}
