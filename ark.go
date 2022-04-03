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

package ark

import (
	"context"
	"net/url"
	"strings"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-ark/database/driver"
)

// Mapper Data mapper for various backends
type Mapper struct {
	db    database.Driver
	cache *ristretto.Cache
}

// New Create a new mapper
func New(dsn string, opts ...database.Option) (*Mapper, error) {
	m := defaultMapper()
	databaseURL, err := url.Parse(dsn)
	if err != nil {
		return nil, trail.Stacktrace(err)
	}

	dialect := strings.TrimSuffix(databaseURL.Scheme, ":")
	switch dialect {
	case "postgres", "redshift":
		m.db, err = driver.NewSQL(dialect, databaseURL, opts...)
	default:
		return nil, trail.NewErrorf("unrecognized dialect: '%s'", dialect)
	}

	if err == nil {
		err = m.db.Ping(context.Background())
	}

	return &m, err
}

// DocumentDecoder decodes a database document.
type DocumentDecoder interface {
	Decode(ctx context.Context, fn func(v interface{}) error) error
}

// DocumentEncoder encodes a database document
type DocumentEncoder interface {
	Encode() interface{}
}

type transientDocument struct {
	v interface{}
}

func (d transientDocument) Encode() interface{} {
	return d.v
}

func (d transientDocument) Decode(_ context.Context, fn func(v interface{}) error) error {
	return fn(d.v)
}

func newTransientDocument(v interface{}) *transientDocument {
	return &transientDocument{v: v}
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
