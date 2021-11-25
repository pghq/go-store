package ark

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/client"
)

const (
	// positiveTTL is the positive ttl for search queries
	positiveTTL = 5 * time.Second

	// negativeTTL is the negative ttl for search queries
	negativeTTL = 30 * time.Second
)

// Query gets a new query for searching the repository.
func (s *Store) Query() client.Query {
	return s.client.Query()
}

// Get for items in store
func (s *Store) Get(ctx context.Context, query client.Query, dst interface{}, opts ...Option) (bool, error) {
	tx, err := s.Context(ctx, opts...)
	if err != nil {
		return false, tea.Error(err)
	}

	return tx.Search(query, dst, opts...)
}

// Search retrieves items from the repository matching criteria.
func (ctx *Context) Search(query client.Query, dst interface{}, opts ...Option) (bool, error) {
	conf := Config{}
	for _, opt := range opts {
		opt.Apply(&conf)
	}

	key := query.String()
	if !conf.consistent {
		v, present := ctx.store.cache.Get(key)
		if present {
			if err, ok := v.(error); ok {
				return true, tea.Error(err)
			}

			if b, ok := v.([]byte); ok {
				dec := gob.NewDecoder(bytes.NewReader(b))
				return true, dec.Decode(dst)
			}
		}
	}

	if _, err := ctx.tx.Execute(query, dst); err != nil {
		if !tea.IsFatal(err) {
			ctx.store.cache.SetWithTTL(key, err, 1, negativeTTL)
		}

		return false, tea.Error(err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(dst)
	ctx.store.cache.SetWithTTL(key, buf.Bytes(), 1, positiveTTL)
	return false, err
}
