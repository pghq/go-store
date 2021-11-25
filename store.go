// Copyright 2021 PGHQ. All Rights Reserved.
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

// Package ark provides a repository implementation.
package ark

import (
	"database/sql"
	"io/fs"
	"reflect"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/client"
)

const (
	// DefaultMaxConns is the default maximum number of open connections.
	DefaultMaxConns = 100
)

// Store is an instance of a postgres Database
type Store struct {
	client client.Client
	cache  *ristretto.Cache
}

// NewStore creates a new data store instance.
func NewStore(primary string, opts ...Option) (*Store, error) {
	var store Store
	conf := Config{
		primary:   primary,
		secondary: primary,
		maxConns:  DefaultMaxConns,
	}
	for _, opt := range opts {
		opt.Apply(&conf)
	}

	switch {
	case conf.client != nil:
		store.client = conf.client
	case strings.HasPrefix(primary, "postgres://"):
		store.client = &pgClient{conf: conf, open: sql.Open, connect: pgxpool.ConnectConfig}
	default:
		return nil, tea.NewError("ambiguous client")
	}

	if err := store.client.Connect(); err != nil {
		return nil, tea.Error(err)
	}

	store.cache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})

	return &store, nil
}

// Fields gets all fields for datastore item(s)
func Fields(args ...interface{}) []string {
	var fields []string
	for _, arg := range args {
		switch v := arg.(type) {
		case []string:
			if len(v) > 0 {
				return v
			}
		case string:
			fields = append(fields, v)
		default:
			if i, err := Item(v, true); err == nil {
				for field, _ := range i {
					fields = append(fields, field)
				}
			}
		}
	}

	return fields
}

// Item converts a struct to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
func Item(in interface{}, transient ...interface{}) (map[string]interface{}, error) {
	if v, ok := in.(map[string]interface{}); ok {
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, tea.NewErrorf("item of type %T is not a struct", v)
	}

	item := make(map[string]interface{})
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if key := field.Tag.Get("db"); key != "" && key != "-" {
			if len(transient) == 0 && strings.HasSuffix(key, ",transient") {
				continue
			}

			item[strings.Split(key, ",")[0]] = v.Field(i).Interface()
		}
	}

	return item, nil
}

// Config is a configuration for the store
type Config struct {
	consistent         bool
	client             client.Client
	primary            string
	secondary          string
	ro                 bool
	maxConns           int
	maxConnLifetime    time.Duration
	migrationFS        fs.FS
	migrationDirectory string
}

// Option sets a single configuration for the client
type Option interface {
	Apply(conf *Config)
}

type secondary string

func (s secondary) Apply(conf *Config) {
	if conf != nil {
		conf.secondary = string(s)
	}
}

// Secondary sets the secondary data source
func Secondary(dsn string) Option {
	return secondary(dsn)
}

type maxConns int

func (c maxConns) Apply(conf *Config) {
	if conf != nil {
		conf.maxConns = int(c)
	}
}

// MaxConns sets the max number of open connections
func MaxConns(count int) Option {
	return maxConns(count)
}

type maxConnLifetime time.Duration

func (l maxConnLifetime) Apply(conf *Config) {
	if conf != nil {
		conf.maxConnLifetime = time.Duration(l)
	}
}

// MaxConnLifetime sets the max connection lifetime
func MaxConnLifetime(ttl time.Duration) Option {
	return maxConnLifetime(ttl)
}

type migrationFS struct {
	fs fs.FS
}

func (fs migrationFS) Apply(conf *Config) {
	if conf != nil {
		conf.migrationFS = fs.fs
	}
}

// MigrationFS sets the migrations file system
func MigrationFS(fs fs.FS) Option {
	return migrationFS{fs: fs}
}

type migrationDirectory string

func (d migrationDirectory) Apply(conf *Config) {
	if conf != nil {
		conf.migrationDirectory = string(d)
	}
}

// MigrationDirectory sets the migrations directory
func MigrationDirectory(directory string) Option {
	return migrationDirectory(directory)
}

type rawClient struct {
	client client.Client
}

func (c rawClient) Apply(conf *Config) {
	if conf != nil {
		conf.client = c.client
	}
}

// RawClient sets the raw client for the store
func RawClient(c client.Client) Option {
	return rawClient{client: c}
}

type consistent bool

func (c consistent) Apply(conf *Config) {
	if conf != nil {
		conf.consistent = bool(c)
	}
}

// Consistent notifies the search to not use cached results
func Consistent() Option {
	return consistent(true)
}

type ro bool

func (o ro) Apply(conf *Config) {
	if conf != nil {
		conf.ro = bool(o)
	}
}

// ReadOnly sets the search to read only
func ReadOnly() Option {
	return ro(true)
}
