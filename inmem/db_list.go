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
		return tea.NewError("write only")
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

	tbl := tx.Table(table)
	io := tbl.IteratorOptions(query)
	it := tx.reader.NewIterator(io.badger)
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
		if io.indexing {
			var err error
			key := item.Key()
			pk := bytes.TrimPrefix(key, io.badger.Prefix)
			if item, err = tx.reader.Get(pk); err != nil {
				if err == badger.ErrKeyNotFound {
					err = tea.NotFound(err)
				}
				return tea.Error(err)
			}
		}

		doc := tbl.NewDocument(item.Key())
		if err := item.Value(func(b []byte) error { return doc.Decode(b) }); err != nil {
			return tea.Error(err)
		}

		rv := reflect.New(reflect.TypeOf(v).Elem().Elem())
		v := rv.Interface()
		if !doc.Matches(query, v) {
			continue
		}

		if err := doc.Copy(v); err != nil {
			return tea.Error(err)
		}

		values = append(values, rv.Elem())
		i += 1
	}

	if len(values) == 0 {
		if query.Limit == 1 {
			return tea.NewNotFound("not found")
		}

		return tea.NewNoContent("not found")
	}

	rv.Set(reflect.Append(rv, values...))
	return nil
}
