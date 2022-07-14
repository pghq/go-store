// Copyright 2022 PGHQ. All Rights Reserved.
//
// Licensed under the GNU General Public License, Version 3 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-store/provider"
	"github.com/pghq/go-store/provider/sql"
)

type contextKey = struct{}

// Store an abstraction over database persistence
type Store struct {
	db    provider.Provider
	cache *ristretto.Cache
}

// Begin a transaction
func (s Store) Begin(ctx context.Context, opts ...provider.TxOption) (Txn, error) {
	return begin(ctx, &s, opts...)
}

// Do execute callback in a transaction
func (s Store) Do(ctx context.Context, fn func(tx Txn) error, opts ...provider.TxOption) error {
	tx, err := begin(ctx, &s, opts...)
	if err != nil {
		return trail.Stacktrace(err)
	}

	defer tx.rollback()
	if err := fn(tx); err != nil {
		return trail.Stacktrace(err)
	}

	return tx.commit()
}

// Batch read
func (s Store) Batch(ctx context.Context, opts ...provider.TxOption) error {
	panic("not implemented")
}

// First retrieve the first value matching the spec
func (s Store) First(ctx context.Context, spec provider.Spec, v interface{}, opts ...QueryOption) error {
	span := trail.StartSpan(ctx, "Store.First")
	defer span.Finish()

	conf := QueryConfig{}
	for _, opt := range opts {
		opt(&conf)
	}

	cv, present := s.cache.Get(spec.Id())
	span.Tags.Set("Store.CacheHit", fmt.Sprintf("%t", present))
	if present {
		return hydrate(v, cv)
	}

	if err := s.db.Repository().First(ctx, spec, v); err != nil {
		return trail.Stacktrace(err)
	}

	if conf.QueryTTL != 0 {
		s.cache.SetWithTTL(spec.Id(), v, 1, conf.QueryTTL)
	}

	return nil
}

// List retrieves a listing of values
func (s Store) List(ctx context.Context, spec provider.Spec, v interface{}, opts ...QueryOption) error {
	span := trail.StartSpan(ctx, "Store.List")
	defer span.Finish()

	conf := QueryConfig{}
	for _, opt := range opts {
		opt(&conf)
	}

	cv, present := s.cache.Get(spec.Id())
	span.Tags.Set("Store.CacheHit", fmt.Sprintf("%t", present))
	if present {
		return hydrate(v, cv)
	}

	if err := s.db.Repository().List(ctx, spec, v); err != nil {
		return trail.Stacktrace(err)
	}

	if conf.QueryTTL != 0 {
		s.cache.SetWithTTL(spec.Id(), v, 1, conf.QueryTTL)
	}

	return nil
}

// Add appends a value to the collection
func (s Store) Add(ctx context.Context, collection string, v interface{}) error {
	span := trail.StartSpan(ctx, "Store.Add")
	defer span.Finish()

	return s.db.Repository().Add(ctx, collection, v)
}

// Edit updates value(s) in the collection
func (s Store) Edit(ctx context.Context, collection string, spec provider.Spec, v interface{}) error {
	span := trail.StartSpan(ctx, "Store.Edit")
	defer span.Finish()

	return s.db.Repository().Edit(ctx, collection, spec, v)
}

// Remove deletes values(s) in the collection
func (s Store) Remove(ctx context.Context, collection string, spec provider.Spec) error {
	span := trail.StartSpan(ctx, "Store.Remove")
	defer span.Finish()

	s.cache.Del(spec.Id())
	return s.db.Repository().Remove(ctx, collection, spec)
}

// NewStore creates a new store instance
func NewStore(db provider.Provider) *Store {
	s := Store{}
	s.cache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	s.db = db
	return &s
}

// New creates a new instance of the data store
func New(opts ...Option) (*Store, error) {
	conf := Config{
		DSN: os.Getenv("DATABASE_URL"),
	}

	for _, opt := range opts {
		opt(&conf)
	}

	prov, err := sql.New(conf.Dialect, conf.DSN, conf.Migration, conf.SQLOptions...)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	return NewStore(prov), nil
}

// Txn A unit of work
type Txn struct {
	ctx   context.Context
	uow   provider.UnitOfWork
	store *Store
	root  bool
	done  bool
}

// Context gets the context of the transaction
func (tx Txn) Context() context.Context {
	return tx.ctx
}

// First retrieve the first value matching the spec
func (tx Txn) First(spec provider.Spec, v interface{}, opts ...QueryOption) error {
	return tx.store.First(tx.Context(), spec, v, opts...)
}

// List retrieves a listing of values
func (tx Txn) List(spec provider.Spec, v interface{}, opts ...QueryOption) error {
	return tx.store.List(tx.Context(), spec, v, opts...)
}

// Add appends a value to the collection
func (tx Txn) Add(collection string, v interface{}) error {
	return tx.store.Add(tx.Context(), collection, v)
}

// Edit updates value(s) in the collection
func (tx Txn) Edit(collection string, spec provider.Spec, v interface{}) error {
	return tx.store.Edit(tx.Context(), collection, spec, v)
}

// Remove deletes values(s) in the collection
func (tx Txn) Remove(collection string, spec provider.Spec) error {
	return tx.store.Remove(tx.Context(), collection, spec)
}

// commit submit a unit of work
func (tx *Txn) commit() error {
	if tx.done || !tx.root {
		return nil
	}

	tx.done = true
	return tx.uow.Commit(tx.Context())
}

// rollback cancel a unit of work
func (tx *Txn) rollback() {
	if !tx.done && tx.root {
		tx.done = true
		tx.uow.Rollback(tx.Context())
	}
}

// Config a configuration for the store
type Config struct {
	Dialect    string
	DSN        string
	Migration  fs.ReadDirFS
	SQLOptions []sql.Option
}

// Option A store configuration option
type Option func(conf *Config)

// WithMigration Use database migration
func WithMigration(fs fs.ReadDirFS) Option {
	return func(conf *Config) {
		conf.Migration = fs
	}
}

// WithSQL Use custom sql options
func WithSQL(opts ...sql.Option) Option {
	return func(conf *Config) {
		conf.SQLOptions = opts
	}
}

// WithDSN Use dsn
func WithDSN(dialect, dsn string) Option {
	return func(conf *Config) {
		conf.Dialect = dialect
		conf.DSN = dsn
	}
}

// QueryConfig configuration for store queries
type QueryConfig struct {
	QueryTTL time.Duration
}

// QueryOption for customizing store queries
type QueryOption func(conf *QueryConfig)

// QueryTTL custom duration of time to cache queries for
func QueryTTL(duration time.Duration) QueryOption {
	return func(conf *QueryConfig) {
		conf.QueryTTL = duration
	}
}

// begin create instance of a read/write database transaction
func begin(ctx context.Context, store *Store, opts ...provider.TxOption) (Txn, error) {
	if tx, ok := ctx.Value(contextKey{}).(Txn); ok {
		tx.root = false
		tx.ctx = context.WithValue(ctx, contextKey{}, tx)
		return tx, nil
	}

	uow, err := store.db.Begin(ctx, opts...)
	if err != nil {
		return Txn{}, trail.Stacktrace(err)
	}

	tx := Txn{
		uow:  uow,
		root: true,
	}

	tx.ctx = context.WithValue(ctx, contextKey{}, tx)
	return tx, nil
}

// hydrate Copies src value to destination
func hydrate(dst, src interface{}) error {
	dv := reflect.Indirect(reflect.ValueOf(dst))
	sv := reflect.Indirect(reflect.ValueOf(src))

	if !dv.CanSet() || dv.Type() != sv.Type() {
		return trail.NewError("bad hydration")
	}

	dv.Set(sv)
	return nil
}
