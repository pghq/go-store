package inmem

import (
	"bytes"
	"reflect"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/db"
)

func (tx txn) List(table string, v interface{}, opts ...db.QueryOption) error {
	if tx.reader == nil {
		return tea.NewError("not a read capable tx")
	}

	query := db.QueryWith(opts)
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return tea.NewError("dst must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return tea.NewError("dst must be a pointer to slice")
	}

	tbl, err := tx.table(table)
	if err != nil {
		return tea.Error(err)
	}

	io := badger.DefaultIteratorOptions
	if len(query.Eq) > 0 {
		eq := query.Eq[0]
		for indexName, indexValue := range eq {
			index, err := tbl.index(indexName, indexValue)
			if err != nil {
				return tea.Error(err)
			}
			io.Prefix = index.prefix
		}
	}

	it := tx.reader.NewIterator(io)
	defer it.Close()

	var values []reflect.Value
	start := query.Limit * query.Page
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		if len(values) == query.Limit {
			break
		}

		if i < start {
			i += 1
		}

		item := it.Item()
		if io.Prefix != nil {
			key := item.Key()
			pk := bytes.TrimPrefix(key, io.Prefix)
			if item, err = tx.reader.Get(pk); err != nil {
				return tea.Error(err)
			}
		}

		rv := reflect.New(reflect.TypeOf(v).Elem().Elem())
		if err := item.Value(func(b []byte) error { return db.Decode(b, &rv) }); err != nil {
			return tea.Error(err)
		}

		values = append(values, rv.Elem())
		i += 1
	}

	if len(values) == 0 {
		return tea.NewNoContent("not found")
	}

	rv.Set(reflect.Append(rv, values...))
	return nil
}
