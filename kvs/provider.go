package kvs

import (
	"context"

	"github.com/dgraph-io/badger/v3"
)

// Provider is a low-level in-memory KVS provider.
type Provider struct {
	client *badger.DB
}

func (p *Provider) Connect(_ context.Context) error {
	client, err := badger.Open(badger.DefaultOptions("").WithLogger(nil).WithInMemory(true))
	p.client = client
	return err
}

// NewProvider creates a new in-memory KV store provider
func NewProvider() *Provider {
	return &Provider{}
}
