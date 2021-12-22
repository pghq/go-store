package redis

import (
	"context"
	"reflect"

	"github.com/go-redis/redis/v8"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (d DB) Txn(ctx context.Context, opts ...database.TxnOption) database.Txn {
	config := database.TxnConfigWith(opts)
	unit := d.backend.TxPipeline()
	if config.ReadOnly {
		unit.ReadOnly(ctx)
	}

	return txn{
		ctx:     ctx,
		unit:    unit,
		backend: d.backend,
		reads:   make(chan read, config.BatchReadSize),
	}
}

// txn Redis transaction
type txn struct {
	ctx     context.Context
	unit    redis.Pipeliner
	backend *redis.Client
	reads   chan read
}

func (tx txn) Commit() error {
	defer tx.unit.Close()
	if _, err := tx.unit.Exec(tx.ctx); err != nil {
		if err == redis.Nil {
			return tea.AsErrNotFound(err)
		}
		return tea.Stack(err)
	}

	for {
		select {
		case read := <-tx.reads:
			switch cmd := read.cmd.(type) {
			case *redis.StringCmd:
				b, _ := cmd.Bytes()
				if err := database.Decode(b, read.v); err != nil {
					return tea.Stack(err)
				}
			case *redis.ScanCmd:
				keys, _, _ := cmd.Result()
				_ = tx.backend.MGet(tx.ctx, keys...).Val()
				var values []reflect.Value
				for _, v := range tx.backend.MGet(tx.ctx, keys...).Val() {
					b := []byte(v.(string))
					rv := reflect.New(reflect.TypeOf(read.v).Elem().Elem())
					if err := database.Decode(b, &rv); err != nil {
						return tea.Stack(err)
					}
					values = append(values, rv.Elem())
				}

				if len(values) == 0 {
					if read.limit == 1 {
						return tea.ErrNotFound("not found")
					}

					return tea.ErrNoContent("not found")
				}

				rv := reflect.ValueOf(read.v).Elem()
				rv.Set(reflect.Append(rv, values...))
			}
		default:
			return nil
		}
	}
}

func (tx txn) Rollback() error {
	defer tx.unit.Close()
	return tx.unit.Discard()
}

// read A single read from the redis database
type read struct {
	v     interface{}
	cmd   redis.Cmder
	limit int
}
