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

package ark

import (
	"context"

	"github.com/dgraph-io/ristretto"

	"github.com/pghq/go-ark/db"
	"github.com/pghq/go-ark/inmem"
	"github.com/pghq/go-ark/redis"
	"github.com/pghq/go-ark/sql"
)

// Mapper Data mapper for various backends
type Mapper struct {
	config []db.Option
	db     db.DB
	err    error
	cache  *ristretto.Cache
}

// SetError exposes any underlying mapper errors
func (m *Mapper) SetError(err error) {
	m.err = err
}

// Error exposes any underlying mapper errors
func (m Mapper) Error() error {
	return m.err
}

// WithOpts Configure mapper with custom ops
func (m Mapper) WithOpts(opts []Option) Mapper {
	for _, opt := range opts {
		opt(&m)
	}

	return m
}

// New Create a new mapper
func New(opts ...Option) *Mapper {
	m := defaultMapper().WithOpts(opts)
	if m.db == nil {
		m.db = inmem.NewDB(m.config...)
	}

	m.err = m.db.Ping(context.Background())
	return &m
}

// NewSQL Creates a new SQL mapper
func NewSQL(opts ...db.Option) *Mapper {
	return New(DB(sql.NewDB(opts...)))
}

// NewRedis Creates a new Redis mapper
func NewRedis(opts ...db.Option) *Mapper {
	return New(DB(redis.NewDB(opts...)))
}

// NewRDB Creates a new in-memory RDB mapper
func NewRDB(schema db.Schema, opts ...db.Option) *Mapper {
	return New(DB(inmem.NewDB(append(opts, db.RDB(schema))...)))
}

// defaultMapper create a new default mapper
func defaultMapper() Mapper {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})

	return Mapper{
		cache: cache,
	}
}

// Option for Mapper
type Option func(m *Mapper)

// DB mapper option
func DB(o db.DB) Option {
	return func(m *Mapper) {
		m.db = o
	}
}
