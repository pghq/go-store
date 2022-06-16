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

const (
	// ViewTTL time to live for cached results
	ViewTTL = 100 * time.Millisecond
)

type contextKey = struct{}

// Store an abstraction over database persistence
type Store struct {
	provider provider.Provider
	cache    *ristretto.Cache
}

// Do execute callback in a transaction
func (s Store) Do(ctx context.Context, fn func(tx Txn) error, opts ...provider.TxOption) error {
	tx, err := begin(ctx, s.provider, s.cache, opts...)
	if err != nil {
		return trail.Stacktrace(err)
	}

	defer tx.rollback()
	if err := fn(tx); err != nil {
		return trail.Stacktrace(err)
	}

	return tx.commit()
}

// NewStore creates a new store instance
func NewStore(provider provider.Provider) *Store {
	s := Store{}
	s.cache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	s.provider = provider
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

	prov, err := sql.New(conf.Dialect, conf.DSN, sql.WithMigration(conf.Migration))
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	return NewStore(prov), nil
}

// Txn A unit of work
type Txn struct {
	ctx   context.Context
	uow   provider.UnitOfWork
	repo  provider.Repository
	cache *ristretto.Cache
	root  bool
	done  bool
}

// Context gets the context of the transaction
func (tx Txn) Context() context.Context {
	return tx.ctx
}

// First retrieve the first value matching the spec
func (tx Txn) First(spec provider.Spec, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Repository.First")
	defer span.Finish()

	cv, present := tx.cache.Get(spec.Id())
	span.Tags.Set("Repository.CacheHit", fmt.Sprintf("%t", present))
	if present {
		return hydrate(v, cv)
	}

	if err := tx.repo.First(tx.Context(), spec, v); err != nil {
		return trail.Stacktrace(err)
	}

	tx.cache.SetWithTTL(spec.Id(), v, 1, ViewTTL)
	return nil
}

// List Retrieve a listing of values
func (tx Txn) List(spec provider.Spec, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Repository.List")
	defer span.Finish()

	cv, present := tx.cache.Get(spec.Id())
	span.Tags.Set("Repository.CacheHit", fmt.Sprintf("%t", present))
	if present {
		return hydrate(v, cv)
	}

	if err := tx.repo.List(tx.Context(), spec, v); err != nil {
		return trail.Stacktrace(err)
	}

	tx.cache.SetWithTTL(spec.Id(), v, 1, ViewTTL)
	return nil
}

// Add a value to the repository
func (tx Txn) Add(collection string, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Repository.Add")
	defer span.Finish()

	return tx.repo.Add(tx.Context(), collection, v)
}

// Edit update value(s) in the repository
func (tx Txn) Edit(spec provider.Spec, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Repository.Edit")
	defer span.Finish()

	return tx.repo.Edit(tx.Context(), spec, v)
}

// Remove Delete a value by key
func (tx Txn) Remove(spec provider.Spec) error {
	span := trail.StartSpan(tx.Context(), "Repository.Remove")
	defer span.Finish()

	tx.cache.Del(spec.Id())
	return tx.repo.Remove(tx.Context(), spec)
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
	Dialect   string
	DSN       string
	Migration fs.ReadDirFS
}

// Option A store configuration option
type Option func(conf *Config)

// WithMigration Use database migration
func WithMigration(fs fs.ReadDirFS) Option {
	return func(conf *Config) {
		conf.Migration = fs
	}
}

// WithDSN Use dsn
func WithDSN(dialect, dsn string) Option {
	return func(conf *Config) {
		conf.Dialect = dialect
		conf.DSN = dsn
	}
}

// begin create instance of a read/write database transaction
func begin(ctx context.Context, provider provider.Provider, cache *ristretto.Cache, opts ...provider.TxOption) (Txn, error) {
	if tx, ok := ctx.Value(contextKey{}).(Txn); ok {
		tx.root = false
		tx.ctx = context.WithValue(ctx, contextKey{}, tx)
		return tx, nil
	}

	uow, err := provider.Begin(ctx, opts...)
	if err != nil {
		return Txn{}, trail.Stacktrace(err)
	}

	tx := Txn{
		cache: cache,
		uow:   uow,
		repo:  provider.Repository(),
		root:  true,
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
