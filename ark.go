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
	"net/url"
	"strings"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-ark/memory"
	"github.com/pghq/go-ark/redis"
	"github.com/pghq/go-ark/sql"
)

const (
	// Version of the mapper
	Version = "0.0.74"
)

// Mapper Data mapper for various backends
type Mapper struct {
	db    database.DB
	err   error
	cache *ristretto.Cache
}

// SetError sets a mapper error
func (m *Mapper) SetError(err error) {
	m.err = err
}

// Error exposes any underlying mapper errors
func (m Mapper) Error() error {
	return m.err
}

// New Create a new mapper
func New(dsn string, opts ...database.Option) *Mapper {
	m := defaultMapper()
	databaseURL, err := url.Parse(dsn)
	if err != nil {
		m.err = tea.Stacktrace(err)
		return &m
	}

	dialect := strings.TrimSuffix(databaseURL.Scheme, ":")
	switch dialect {
	case "postgres", "redshift":
		m.db = sql.NewDB(dialect, databaseURL, opts...)
	case "redis":
		m.db = redis.NewDB(databaseURL)
	case "memory":
		m.db = memory.NewDB(opts...)
	default:
		m.err = tea.Err("unrecognized dialect: '", dialect, "'")
		return &m
	}

	m.err = m.db.Ping(context.Background())
	return &m
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
