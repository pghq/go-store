package ark

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
)

const (
	// cacheTL is the positive ttl for search queries
	cacheTTL = 500 * time.Millisecond
)

// txn begins a transaction
func (m *Mapper) txn(ctx context.Context, ro ...bool) (*txn, error) {
	t, err := m.provider.Txn(ctx, ro...)
	if err != nil {
		return nil, tea.Error(err)
	}

	tx := txn{
		Context:  ctx,
		cache:    m.cache,
		provider: t,
		root:     true,
	}

	return &tx, nil
}

// txn is an instance of a transaction
type txn struct {
	context.Context
	root     bool
	cache    *ristretto.Cache
	provider internal.Txn
	mutex    sync.Mutex
	pending  []*view
}

// update modifies the database
func (tx *txn) update(statement internal.Stmt, args ...interface{}) internal.Resolver {
	return tx.provider.Exec(statement, args...)
}

// view reads from database (and caches results)
func (tx *txn) view(statement internal.Stmt, dst interface{}, consistent ...bool) internal.Resolver {
	key := statement.Bytes()
	ice := tx.cache != nil && key != nil && (len(consistent) == 0 || !consistent[0])
	if ice {
		if v, present := tx.cache.Get(key); present {
			return v.(*view).Resolve(dst)
		}
	}

	res := tx.provider.Exec(statement, dst)
	if ice && dst != nil {
		tx.mutex.Lock()
		defer tx.mutex.Unlock()

		tx.pending = append(tx.pending, newView(key, dst, res))
	}

	return res
}

// Commit a transaction
func (tx *txn) Commit() error {
	if tx.root {
		tx.mutex.Lock()
		defer tx.mutex.Unlock()

		if err := tx.provider.Commit(); err != nil {
			return tea.Error(err)
		}

		defer func() { tx.pending = nil }()

		for _, i := range tx.pending {
			if _, err := i.Resolver.Resolve(); err == nil {
				tx.cache.SetWithTTL(i.Key, i, 1, cacheTTL)
			}
		}
	}

	return nil
}

// Rollback a transaction
func (tx *txn) Rollback() error {
	var err error
	if tx.root {
		err = tx.provider.Rollback()
	}

	return err
}

// view houses cached values
type view struct {
	Key      []byte
	Value    interface{}
	Resolver internal.Resolver
}

func (v *view) Resolve(dst interface{}) internal.Resolver {
	if v.Value != nil {
		rv := reflect.ValueOf(dst)
		kind := rv.Kind()
		if kind == reflect.Ptr || kind == reflect.Interface{
			if rv := rv.Elem(); rv.CanSet(){
				rv.Set(reflect.ValueOf(v.Value).Elem())
			}
		}
	}

	return v.Resolver
}

func newView(k []byte, dst interface{}, res internal.Resolver) *view {
	v := view{
		Key:      k,
		Value:    dst,
		Resolver: res,
	}

	return &v
}
