package kvs

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
	"github.com/pghq/go-ark/internal/z"
)

func (p *Provider) Txn(ctx context.Context, ro ...bool) (internal.Txn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	txn := kvsTxn{
		ctx:      ctx,
		provider: p.client.NewTransaction(len(ro) == 0 || ro[0] == false),
	}

	return &txn, nil
}

// kvsTxn is a database transaction for in-memory KVS provider
type kvsTxn struct {
	ctx      context.Context
	provider *badger.Txn
}

func (t *kvsTxn) Exec(statement internal.Stmt, args ...interface{}) internal.Resolver {
	m := statement.StandardMethod()
	if m.Key == nil && (m.Insert || m.Update || m.Get || m.Remove) {
		return internal.ExecResponse(0, tea.NewError("missing key"))
	}

	if m.List {
		return internal.ExecResponse(0, tea.NewError("unsupported op"))
	}

	if m.Get && len(args) == 0 {
		return internal.ExecResponse(0, tea.NewError("missing destination"))
	}

	if m.Insert || m.Update {
		b, err := z.Encode(m.Value)
		if err != nil {
			return internal.ExecResponse(0, tea.Error(err))
		}

		entry := badger.NewEntry(m.Key, b)
		if len(args) > 0 {
			if ttl, ok := args[0].(time.Duration); ok {
				entry = entry.WithTTL(ttl)
			}
		}

		return internal.ExecResponse(1, t.provider.SetEntry(entry))
	}

	if m.Remove {
		return internal.ExecResponse(1, t.provider.Delete(m.Key))
	}

	item, err := t.provider.Get(m.Key)
	if item != nil {
		err = item.Value(func(b []byte) error { return z.Decode(b, args[0]) })
	}

	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = tea.NoContent(err)
		}
		return internal.ExecResponse(0, err)
	}

	return internal.ExecResponse(1, nil)
}

func (t *kvsTxn) Commit() error {
	return t.provider.Commit()

}
func (t *kvsTxn) Rollback() error {
	t.provider.Discard()
	return nil
}
