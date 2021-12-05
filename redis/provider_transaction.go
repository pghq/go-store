package redis

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/internal"
)

func (p *Provider) Txn(ctx context.Context, ro ...bool) (internal.Txn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	t := txn{ctx: ctx, pipe: p.client.TxPipeline()}
	pipe := p.client.TxPipeline()
	if len(ro) > 0 && ro[0] {
		t.cmdResolver(pipe.ReadOnly(ctx))
	}

	return &t, nil
}

// txn is an instance of internal.Txn for Redis.
type txn struct {
	ctx       context.Context
	pipe      redis.Pipeliner
	mutex     sync.RWMutex
	resolvers []*resolver
}

func (t *txn) Exec(statement internal.Stmt, args ...interface{}) internal.Resolver {
	m := statement.StandardMethod()
	if m.Key == nil && (m.Insert || m.Update || m.Get || m.Remove) {
		return t.errResolver(tea.NewError("missing key"))
	}

	if m.List {
		return t.errResolver(tea.NewError("unsupported op"))
	}

	if m.Get && len(args) == 0 {
		return t.errResolver(tea.NewError("missing destination"))
	}

	if m.Insert || m.Update {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(m.Value); err != nil {
			return t.errResolver(tea.Error(err))
		}

		var ex time.Duration
		if len(args) > 0 {
			if d, ok := args[0].(time.Duration); ok {
				ex = d
			}
		}
		return t.cmdResolver(t.pipe.Set(t.ctx, string(m.Key), buf.Bytes(), ex))
	}

	if m.Remove {
		return t.cmdResolver(t.pipe.Del(t.ctx, string(m.Key)))
	}

	return t.queryResult(t.pipe.Get(t.ctx, string(m.Key)), args[0])
}

func (t *txn) Commit() error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	defer t.pipe.Close()

	_, err := t.pipe.Exec(t.ctx)
	for _, res := range t.resolvers {
		res.Decode()
	}

	if err != nil {
		if err == redis.Nil {
			err = tea.NoContent(err)
		}
		return tea.Error(err)
	}

	return nil

}
func (t *txn) Rollback() error {
	defer t.pipe.Close()
	return t.pipe.Discard()
}

func (t *txn) errResolver(err error) *resolver {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	res := resolver{err: err}
	t.resolvers = append(t.resolvers, &res)
	return &res
}

func (t *txn) cmdResolver(cmd redis.Cmder) *resolver {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	res := resolver{cmd: cmd, delta: 1}
	t.resolvers = append(t.resolvers, &res)
	return &res
}

func (t *txn) queryResult(query *redis.StringCmd, dst interface{}) *resolver {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	res := resolver{query: query, delta: 1, dst: dst}
	t.resolvers = append(t.resolvers, &res)
	return &res
}

type resolver struct {
	delta int
	err   error
	dst   interface{}
	cmd   redis.Cmder
	query *redis.StringCmd
}

func (r *resolver) Resolve() (int, error) {
	return r.delta, r.err
}

func (r *resolver) Decode() {
	switch {
	case r.cmd != nil:
		r.err = r.cmd.Err()
	case r.query != nil:
		var b []byte
		b, r.err = r.query.Bytes()
		if b != nil {
			dec := gob.NewDecoder(bytes.NewReader(b))
			r.err = dec.Decode(r.dst)
		}
	}

	if r.err != nil {
		r.delta = 0
	}
}
