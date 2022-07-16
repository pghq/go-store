package provider

import (
	"context"

	"github.com/Masterminds/squirrel"
)

var _ Spec = spec{}

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
	One(ctx context.Context, spec Spec, v interface{}) error
	All(ctx context.Context, spec Spec, v interface{}) error
	Add(ctx context.Context, collection string, v interface{}) error
	Edit(ctx context.Context, collection string, spec Spec, v interface{}) error
	Remove(ctx context.Context, collection string, spec Spec) error
	BatchQuery(ctx context.Context, query BatchQuery) error
}

// Spec for querying objects
type Spec interface {
	Id() interface{}
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

type spec struct {
	id      interface{}
	sqlizer squirrel.Sqlizer
}

func (s spec) Id() interface{} {
	return s.id
}

func (s spec) ToSql() (string, []interface{}, error) {
	return s.sqlizer.ToSql()
}

// NewSpec is a helper for creating a spec
func NewSpec(id interface{}, sqlizer squirrel.Sqlizer) Spec {
	return spec{
		id:      id,
		sqlizer: sqlizer,
	}
}
