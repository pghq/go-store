package inmem

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/db"
)

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("can ping db", func(t *testing.T) {
		d := NewDB()
		assert.NotNil(t, d)
		assert.Nil(t, d.Ping(context.TODO()))
	})

	t.Run("can set schema", func(t *testing.T) {
		d := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}}))
		assert.Equal(t, db.Schema{"tests": {"name": {"name"}}}, d.schema)
	})
}

func TestDB_Txn(t *testing.T) {
	d := NewDB()

	t.Run("write", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("batch write", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.BatchWrite())
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

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", "bar")
		assert.NotNil(t, err)
	})

	t.Run("bad document schema", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad document schemaless", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", "foo", func() {})
		assert.NotNil(t, err)
	})

	t.Run("missing index", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{})
		assert.Nil(t, err)
	})

	t.Run("bad index value", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"name": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("bad composite entry", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		_ = tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"})
		assert.NotNil(t, err)
	})

	t.Run("too many indexes", func(t *testing.T) {
		schema := db.Schema{"tests": {}}
		value := map[string]interface{}{"name": "bar"}
		for i := 0; i < 100000; i++ {
			name := fmt.Sprintf("name%d", i)
			schema["tests"][name] = []string{name}
			value[name] = "foo"
		}

		tx := NewDB(db.RDB(schema)).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", value)
		assert.NotNil(t, err)
	})

	t.Run("can set composite with ttl", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"}, db.TTL(time.Second))
		assert.Nil(t, err)
	})

	t.Run("can set value with ttl", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", "foo", "bar", db.TTL(time.Second))
		assert.Nil(t, err)
	})

	t.Run("can set without ttl", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", "foo", "bar")
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
		schema := db.Schema{"tests": {"name": {"name"}, "count": {"count", "enabled"}}}
		tx := NewDB(db.RDB(schema)).Txn(context.TODO(), db.BatchWrite())
		defer tx.Rollback()
		for i := 0; i < 25000; i++ {
			err := tx.Insert("tests", fmt.Sprintf("foo%d", i+100), &value{
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
		tx := NewDB().Txn(context.TODO(), db.BatchWrite())
		defer tx.Rollback()
		err := tx.Update("tests", "foo", "bar")
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", "foo", "bar")
		assert.NotNil(t, err)
	})

	t.Run("key not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", "foo", "bar")
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		_ = tx.Insert("", "foo", "bar")
		_ = tx.Rollback()
		err := tx.Update("", "foo", "bar")
		assert.NotNil(t, err)
	})

	t.Run("can update", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", "foo", "bar")
		err := tx.Update("", "foo", "bar")
		assert.Nil(t, err)
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), db.BatchWrite())
		defer tx.Rollback()
		err := tx.Get("tests", "foo", nil)
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", "foo", nil)
		assert.NotNil(t, err)
	})

	t.Run("key not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", "foo", nil)
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		_ = tx.Insert("", "foo", "bar")
		_ = tx.Rollback()
		err := tx.Get("", "foo", nil)
		assert.NotNil(t, err)
	})

	t.Run("bad value", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", "foo", "bar")
		err := tx.Get("", "foo", "")
		assert.NotNil(t, err)
	})

	t.Run("can get", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("", "foo", "bar")
		var value string
		err := tx.Get("", "foo", &value)
		assert.Nil(t, err)
		assert.Equal(t, "bar", value)
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), db.BatchWrite())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("table not found", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		_ = tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"})
		_ = tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("composite corrupt", func(t *testing.T) {
		d := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}}))
		tx := d.Txn(context.TODO())
		_ = tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"})
		_ = tx.Commit()
		_ = d.backend.Update(func(txn *badger.Txn) error {
			pfx, _ := prefix([]byte("tests"), "id")
			return txn.Set(append(pfx, append([]byte{1}, []byte("foo")...)...), nil)
		})
		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("bad composite key", func(t *testing.T) {
		d := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}}))
		tx := d.Txn(context.TODO())
		_ = tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"})
		_ = tx.Commit()
		_ = d.backend.Update(func(txn *badger.Txn) error {
			pfx, _ := prefix([]byte("tests"), "id")
			b, _ := db.Encode([][]byte{[]byte("!badger!")})
			return txn.Set(append(pfx, append([]byte{1}, []byte("foo")...)...), b)
		})
		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("can remove", func(t *testing.T) {
		tx := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}}})).Txn(context.TODO())
		defer tx.Rollback()
		_ = tx.Insert("tests", "foo", map[string]interface{}{"name": "bar"})
		err := tx.Remove("tests", "foo")
		assert.Nil(t, err)
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	d := NewDB(db.RDB(db.Schema{"tests": {"name": {"name"}, "count": {"count", "enabled"}}}))
	tx := d.Txn(context.TODO())
	_ = tx.Insert("tests", "foo1", map[string]interface{}{"name": "bar", "count": 1})
	_ = tx.Insert("tests", "foo2", map[string]interface{}{"name": "baz", "count": 2})
	_ = tx.Insert("tests", "foo3", map[string]interface{}{"name": "qux", "count": 2})
	_ = tx.Commit()

	t.Run("not a read capable tx", func(t *testing.T) {
		tx := NewDB().Txn(context.TODO(), db.BatchWrite())
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
		err := tx.List("tests", &v, db.Eq("count", func() {}))
		assert.NotNil(t, err)
	})

	t.Run("index not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, db.Eq("foo", ""))
		assert.NotNil(t, err)
	})

	t.Run("pk not found", func(t *testing.T) {
		_ = d.backend.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte{196, 244, 4, 229, 84, 195, 52, 33}, nil)
		})
		defer d.backend.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte{196, 244, 4, 229, 84, 195, 52, 33})
		})
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, db.Eq("count", []interface{}{1, nil}))
		assert.NotNil(t, err)
	})

	t.Run("bad slice", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		err := tx.List("tests", &v, db.Eq("count", []interface{}{1, nil}))
		assert.NotNil(t, err)
	})

	t.Run("no limit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, db.Eq("count", []interface{}{1, nil}), db.Limit(0))
		assert.NotNil(t, err)
	})

	t.Run("next page", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var v []map[string]interface{}
		err := tx.List("tests", &v, db.Eq("count", []interface{}{1, nil}), db.Limit(1), db.Page(1))
		assert.Nil(t, err)
	})
}
