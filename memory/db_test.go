package memory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

func TestMain(m *testing.M) {
	tea.Testing()
	os.Exit(m.Run())
}

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("can ping db", func(t *testing.T) {
		d := NewDB()
		assert.NotNil(t, d)
		assert.Nil(t, d.Ping(context.TODO()))
	})

	t.Run("can set schema", func(t *testing.T) {
		d := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}}))
		assert.Equal(t, database.Schema{"tests": {"name": {"name"}}}, d.schema)
	})
}

func TestDB_Txn(t *testing.T) {
	d := NewDB()

	t.Run("write", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := d.Txn(context.TODO(), database.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("batch write", func(t *testing.T) {
		tx := d.Txn(context.TODO(), database.BatchWrite())
		assert.NotNil(t, tx)
	})

	t.Run("can rollback", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
		err := tx.Rollback()
		assert.Nil(t, err)
	})

	t.Run("can commit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
		err := tx.Commit()
		assert.Nil(t, err)
	})
}

func TestTxn_Insert(t *testing.T) {
	t.Parallel()

	t.Run("bad document schema", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id("foo"), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad document schemaless", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", database.Id("foo"), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad index value", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("bad composite entry", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		_ = tx.Rollback()
		err := tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"})
		assert.NotNil(t, err)
	})

	t.Run("too many indexes", func(t *testing.T) {
		schema := database.Schema{"tests": {}}
		value := map[string]interface{}{"name": "bar"}
		for i := 0; i < 100000; i++ {
			name := fmt.Sprintf("name%d", i)
			schema["tests"][name] = []string{name}
			value[name] = "foo"
		}

		tx := NewDB(database.Storage(schema)).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id("foo"), value)
		assert.NotNil(t, err)
	})

	t.Run("can set composite with ttl", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"}, database.Expire(time.Second))
		assert.Nil(t, err)
	})

	t.Run("can set value with ttl", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", database.Id("foo"), "bar", database.Expire(time.Second))
		assert.Nil(t, err)
	})

	t.Run("can set without ttl", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", database.Id("foo"), "bar")
		assert.Nil(t, err)
	})

	t.Run("can set many", func(t *testing.T) {
		type value struct {
			Name      string `db:"name"`
			Latitude  string
			Longitude string
			Count     int  `db:"count"`
			Enabled   bool `db:"enabled"`
		}
		schema := database.Schema{"tests": {"name": {"name"}, "count": {"count", "enabled"}}}
		tx := NewDB(database.Storage(schema)).Txn(context.TODO(), database.BatchWrite())
		defer tx.Rollback()
		for i := 0; i < 25000; i++ {
			err := tx.Insert("tests", database.Id(fmt.Sprintf("foo%d", i+100)), &value{
				Name:      "foo",
				Latitude:  "78.00",
				Longitude: "-78.00",
				Count:     1,
				Enabled:   true,
			})
			if err != nil {
				panic(err)
			}
		}
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), database.BatchWrite())
		defer tx.Rollback()
		err := tx.Update("tests", database.Id("foo"), "bar")
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.Id("foo"), "bar")
		assert.NotNil(t, err)
	})

	t.Run("key not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", database.Id("foo"), "bar")
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		_ = tx.Insert("", database.Id("foo"), "bar")
		_ = tx.Rollback()
		err := tx.Update("", database.Id("foo"), "bar")
		assert.NotNil(t, err)
	})

	t.Run("can update", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", database.Id("foo"), "bar")
		err := tx.Update("", database.Id("foo"), "bar")
		assert.Nil(t, err)
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), database.BatchWrite())
		defer tx.Rollback()
		err := tx.Get("tests", database.Id("foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.Id("foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("key not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", database.Id("foo"), nil)
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("bad decode", func(t *testing.T) {
		d := NewDB()
		d.backend.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("%s", database.Id("foo"))), []byte("bad"))
		})

		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", database.Id("foo"), nil)
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		_ = tx.Insert("", database.Id("foo"), "bar")
		_ = tx.Rollback()
		err := tx.Get("", database.Id("foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("bad value", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", database.Id("foo"), "bar")
		err := tx.Get("", database.Id("foo"), "")
		assert.NotNil(t, err)
	})

	t.Run("can get", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", database.Id("foo"), "bar")
		var value string
		err := tx.Get("", database.Id("foo"), &value)
		assert.Nil(t, err)
		assert.Equal(t, "bar", value)
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), database.BatchWrite())
		defer tx.Rollback()
		err := tx.Remove("tests", database.Id("foo"))
		assert.NotNil(t, err)
	})

	t.Run("key not found", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", database.Id("foo"))
		assert.NotNil(t, err)
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		_ = tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"})
		_ = tx.Rollback()
		err := tx.Remove("tests", database.Id("foo"))
		assert.NotNil(t, err)
	})

	t.Run("composite corrupt", func(t *testing.T) {
		d := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}}))
		tx := d.Txn(context.TODO())
		_ = tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"})
		_ = tx.Commit()
		_ = d.backend.Update(func(txn *badger.Txn) error {
			pfx := prefix([]byte("tests"))
			pfx = prefix(pfx, []byte{1})
			return txn.Set(append(pfx, []byte(fmt.Sprintf("%s", database.Id("foo")))...), nil)
		})
		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", database.Id("foo"))
		assert.NotNil(t, err)
	})

	t.Run("bad composite key", func(t *testing.T) {
		d := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}}))
		tx := d.Txn(context.TODO())
		_ = tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"})
		_ = tx.Commit()
		_ = d.backend.Update(func(txn *badger.Txn) error {
			pfx := prefix([]byte("tests"))
			pfx = prefix(pfx, []byte{1})
			b, _ := database.Encode([][]byte{[]byte("!badger!")})
			return txn.Set(append(pfx, []byte(fmt.Sprintf("%s", database.Id("foo")))...), b)
		})
		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", database.Id("foo"))
		assert.NotNil(t, err)
	})

	t.Run("can remove by key", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"})
		err := tx.Remove("tests", database.Id("foo"))
		assert.Nil(t, err)
	})

	t.Run("can remove by attribute", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {}})).Txn(context.TODO())
		defer tx.Rollback()

		type uuid [16]byte
		type value struct {
			Id *uuid `db:"id"`
		}

		assert.Nil(t, tx.Insert("tests", database.Id("foo"), value{Id: &uuid{1}}))

		var v []value
		assert.Nil(t, tx.List("tests", &v, database.Eq("id", &uuid{1})))

		for range v {
			err := tx.Remove("tests", database.Id("foo"))
			assert.Nil(t, err)
		}
	})

	t.Run("can remove no index", func(t *testing.T) {
		tx := NewDB(database.Storage(database.Schema{"tests": {}})).Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("tests", database.Id("foo"), map[string]interface{}{"name": "bar"}))
		err := tx.Remove("tests", database.Id("foo"))
		assert.Nil(t, err)
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	d := NewDB(database.Storage(database.Schema{"tests": {"name": {"name"}, "count": {"count", "enabled"}}}))
	tx := d.Txn(context.TODO())
	_ = tx.Insert("tests", database.Id("foo1"), map[string]interface{}{"name": "bar", "count": 1})
	_ = tx.Insert("tests", database.Id("foo2"), map[string]interface{}{"name": "baz", "count": 2})
	_ = tx.Insert("tests", database.Id("foo3"), map[string]interface{}{"name": "qux", "count": 2})
	_ = tx.Commit()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), database.BatchWrite())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v)
		assert.NotNil(t, err)
	})

	t.Run("dst must be a pointer", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("tests", "")
		assert.NotNil(t, err)
	})

	t.Run("dst must be a pointer to slice", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v map[string]interface{}
		err := tx.List("tests", &v)
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v)
		assert.NotNil(t, err)
	})

	t.Run("bad index", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("count", func() {}))
		assert.NotNil(t, err)
	})

	t.Run("index not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("foo", ""))
		assert.NotNil(t, err)
	})

	t.Run("bad decode", func(t *testing.T) {
		d := NewDB()
		d.backend.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("foo"), []byte("bad"))
		})

		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("", &v)
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("bad copy", func(t *testing.T) {
		d := NewDB()
		d.backend.Update(func(txn *badger.Txn) error {
			b, _ := database.Encode(Document{})
			return txn.Set([]byte("foo"), b)
		})

		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		err := tx.List("", &v)
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("pk not found", func(t *testing.T) {
		assert.Nil(t, d.backend.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte{170, 192, 24, 205, 201, 54, 12, 77, 1, 2, 3}, nil)
		}))
		defer d.backend.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte{170, 192, 24, 205, 201, 54, 12, 77, 1, 2, 3})
		})
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		assert.NotNil(t, tx.List("tests", &v, database.Eq("count", 1, nil)))
	})

	t.Run("bad slice", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		err := tx.List("tests", &v, database.Eq("count", 1, nil))
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("count", 1337, true), database.Limit(1))
		assert.NotNil(t, err)
	})

	t.Run("no limit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("count", 1, nil), database.Limit(0))
		assert.NotNil(t, err)
	})

	t.Run("ignore nil", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("count", 1), database.Limit(1), database.Page(1))
		assert.Nil(t, err)
	})

	t.Run("next page", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, database.Eq("count", 1, nil), database.Limit(1), database.Page(1))
		assert.Nil(t, err)
	})
}

func TestSubIndex(t *testing.T) {
	t.Parallel()

	t.Run("not equal", func(t *testing.T) {
		i := SubIndex{Columns: []string{"foo"}}
		assert.False(t, i.Equal(map[string]interface{}{"foo": "bar"}, map[string]interface{}{"foo": "baz"}))
	})

	t.Run("bad build value", func(t *testing.T) {
		i := SubIndex{}
		assert.NotNil(t, i.Build(func() {}))
	})
}

func TestAttributes_Contains(t *testing.T) {
	t.Parallel()

	t.Run("not equal", func(t *testing.T) {
		a := Attributes{}
		query := database.QueryWith([]database.QueryOption{database.NotEq("foo", "bar")})
		assert.False(t, a.Contains(query, map[string]interface{}{"foo": "bar"}))
	})

	t.Run("not equal index miss", func(t *testing.T) {
		a := Attributes{"foo": SubIndex{Columns: []string{"foo"}}}
		query := database.QueryWith([]database.QueryOption{database.NotEq("foo", "bar")})
		assert.False(t, a.Contains(query, map[string]interface{}{"foo": "bar"}))
	})

	t.Run("not equal index match", func(t *testing.T) {
		a := Attributes{"foo": SubIndex{Columns: []string{"foo"}}}
		query := database.QueryWith([]database.QueryOption{database.NotEq("foo", "baz")})
		assert.True(t, a.Contains(query, map[string]interface{}{"foo": "bar"}))
	})
}
