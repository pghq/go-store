package memory

import (
	"bytes"
	"reflect"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

func (tx txn) List(table string, v interface{}, args ...interface{}) error {
	if tx.reader == nil {
		return tea.Err("write only")
	}

	req := database.NewRequest(args...)
	span := tea.Nest(tx.ctx, "memory")
	defer span.End()
	span.Tag("operation", "list")
	span.Tag("req", req)

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return tea.Err("dst must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return tea.Err("dst must be a pointer to slice")
	}

	tbl := tx.Table(table)
	io := tbl.IteratorOptions(req)
	it := tx.reader.NewIterator(io.badger)
	defer it.Close()

	var values []reflect.Value
	start := req.Limit * req.Page
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		if len(values) == req.Limit {
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
					err = tea.AsErrNotFound(err)
				}
				return tea.Stack(err)
			}
		}

		doc := tbl.NewDocument(item.Key())
		if err := item.Value(func(b []byte) error { return doc.Decode(b) }); err != nil {
			return tea.Stack(err)
		}

		rv := reflect.New(reflect.TypeOf(v).Elem().Elem())
		v := rv.Interface()
		if !doc.Matches(req, v) {
			continue
		}

		if err := doc.Copy(v); err != nil {
			return tea.Stack(err)
		}

		values = append(values, rv.Elem())
		i += 1
	}

	if len(values) == 0 {
		if req.Limit == 1 {
			return tea.ErrNotFound("not found")
		}

		return tea.ErrNoContent("not found")
	}

	rv.Set(reflect.Append(rv, values...))
	return nil
}
