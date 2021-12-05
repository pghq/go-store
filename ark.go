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
	"context"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
	"github.com/pghq/go-ark/kvs"
	"github.com/pghq/go-ark/rdb"
	"github.com/pghq/go-ark/redis"
	"github.com/pghq/go-ark/sql"
)

// Mapper is a data mapper for various data providers.
type Mapper struct {
	driverName string
	dsn        string
	cache      *ristretto.Cache
	provider   internal.Provider
}

func (m *Mapper) DSN(dsn string) *Mapper {
	m.dsn = dsn
	return m
}

func (m *Mapper) Driver(name string) *Mapper {
	m.driverName = name
	return m
}

// ConnectKVS creates a KVS connection
func (m *Mapper) ConnectKVS(ctx context.Context, provider string, opts ...Option) (*KVSConn, error) {
	conf := Config{}
	for _, opt := range opts {
		opt.Apply(&conf)
	}

	switch provider {
	case "inmem":
		m.provider = kvs.NewProvider()
	case "redis":
		m.provider = redis.NewProvider(m.dsn, conf.redis)
	default:
		return nil, tea.NewError("bad provider")
	}

	if err := m.provider.Connect(ctx); err != nil {
		return nil, tea.Error(err)
	}

	conn := KVSConn{
		mapper: m,
	}

	return &conn, nil
}

// ConnectRDB creates an RDB connection
func (m *Mapper) ConnectRDB(ctx context.Context, provider string, opts ...Option) (*RDBConn, error) {
	conf := Config{}
	for _, opt := range opts {
		opt.Apply(&conf)
	}

	switch provider {
	case "inmem":
		m.provider = rdb.NewProvider(m.dsn)
	case "sql":
		m.provider = sql.NewProvider(m.driverName, m.dsn, conf.sql)
	default:
		return nil, tea.NewError("bad provider")
	}

	if err := m.provider.Connect(ctx); err != nil {
		return nil, tea.Error(err)
	}

	conn := RDBConn{
		mapper: m,
	}

	return &conn, nil
}

// Open a database mapper
func Open() *Mapper {
	m := Mapper{}
	m.cache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})

	return &m
}

// Config is a configuration for the store
type Config struct {
	sql   sql.Config
	redis redis.Config
}

// Option sets a single configuration for the client
type Option interface {
	Apply(conf *Config)
}

// SQLConfig is the SQL configuration option
type SQLConfig sql.Config

func (o SQLConfig) Apply(conf *Config) {
	if conf != nil {
		conf.sql = sql.Config(o)
	}
}

// RedisConfig is the Redis configuration option
type RedisConfig redis.Config

func (o RedisConfig) Apply(conf *Config) {
	if conf != nil {
		conf.redis = redis.Config(o)
	}
}
