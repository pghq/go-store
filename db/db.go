package db

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
)

var _ Spec = spec{}

// DB provides instances of transactions and repositories.
type DB interface {
	Repository() Repository
	Begin(ctx context.Context, opts ...TxOption) (UnitOfWork, error)
}

// UnitOfWork to do
type UnitOfWork interface {
	Tx() interface{}
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
	Batch(ctx context.Context, batch Batch) error
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

// Sqlizer is a helper for creating a spec
func Sqlizer(sqlizer squirrel.Sqlizer) Spec {
	sql, args, _ := sqlizer.ToSql()
	return spec{
		id:      fmt.Sprintf("%s %+v", sql, args),
		sqlizer: sqlizer,
	}
}

// Sql is a helper for creating a spec from an sql literal
func Sql(literal string, args ...interface{}) Spec {
	return sqlLiteral{sql: literal, args: args}
}

type deferSpec struct {
	id interface{}
	fn func() squirrel.Sqlizer
}

func (d *deferSpec) Id() interface{} {
	return d.id
}

func (d *deferSpec) ToSql() (string, []interface{}, error) {
	return d.fn().ToSql()
}

// Defer creates a deferred execution spec
func Defer(id interface{}, fn func() squirrel.Sqlizer) Spec {
	return &deferSpec{id: id, fn: fn}
}

// sqlLiteral sqlizer
type sqlLiteral struct {
	sql  string
	args []interface{}
}

func (s sqlLiteral) Id() interface{} {
	return fmt.Sprintf("%s %+v", s.sql, s.args)
}

func (s sqlLiteral) ToSql() (string, []interface{}, error) {
	return s.sql, s.args, nil
}
