package rdb

import (
	"context"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal"
)

func TestNewProvider(t *testing.T) {
	t.Run("no schema", func(t *testing.T) {
		p := NewProvider("")
		assert.NotNil(t, p.Connect(context.TODO()))
	})

	t.Run("no primary", func(t *testing.T) {
		p := NewProvider(Schema{
			"tests": map[string][]string{},
		})
		assert.NotNil(t, p.Connect(context.TODO()))
	})

	t.Run("one index", func(t *testing.T) {
		p := NewProvider(Schema{
			"tests": map[string][]string{
				"primary": {"id"},
			},
		})
		assert.Nil(t, p.Connect(context.TODO()))
	})

	t.Run("multiple indexes", func(t *testing.T) {
		p := NewProvider(Schema{
			"tests": map[string][]string{
				"primary":  {"id"},
				"failures": {"failures"},
			},
		})
		assert.Nil(t, p.Connect(context.TODO()))
	})

	t.Run("multiple tables", func(t *testing.T) {
		p := NewProvider(Schema{
			"tests": map[string][]string{
				"primary": {"id"},
			},
			"units": map[string][]string{
				"primary": {"id"},
			},
		})
		assert.Nil(t, p.Connect(context.TODO()))
	})
}

func TestRDB_Txn(t *testing.T) {
	type value struct {
		Id        string `db:"id"`
		Latitude  string
		Longitude string
		Count     int  `db:"count"`
		Enabled   bool `db:"enabled"`
	}

	p := NewProvider(Schema{
		"tests": map[string][]string{
			"primary": {"id"},
			"count":   {"count", "enabled"},
		},
	})
	assert.Nil(t, p.Connect(context.TODO()))

	txn, err := p.Txn(context.TODO())
	assert.Nil(t, err)
	assert.NotNil(t, txn)

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()

		_, err := p.Txn(ctx)
		assert.NotNil(t, err)
	})

	t.Run("read only tx", func(t *testing.T) {
		t.Run("no secondary", func(t *testing.T) {
			p := NewProvider(Schema{
				"tests": map[string][]string{
					"primary": {"id"},
				},
			})
			_ = p.Connect(context.TODO())

			txn, _ := p.Txn(context.TODO(), true)

			v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}
			_, err = txn.Exec(internal.Insert{Table: "tests", Value: v}).Resolve()
			assert.NotNil(t, err)

			_, err = txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("primary", "foo")}).Resolve()
			assert.NotNil(t, err)

			_, err = txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}).Resolve()
			assert.NotNil(t, err)
		})

		t.Run("with secondary", func(t *testing.T) {
			txn, _ := p.Txn(context.TODO(), true)

			v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}
			_, err = txn.Exec(internal.Insert{Table: "tests", Value: v}).Resolve()
			assert.NotNil(t, err)

			_, err = txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("primary", "foo")}).Resolve()
			assert.NotNil(t, err)

			_, err = txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}).Resolve()
			assert.NotNil(t, err)
		})
	})

	t.Run("bad filter", func(t *testing.T) {
		ra, err := txn.Exec(internal.List{Table: "units"}, nil).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Get{Table: "tests"}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Get{Table: "tests", Filter: ""}, &[]value{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("no content", func(t *testing.T) {
		var values []string
		ra, err := txn.Exec(internal.List{Table: "tests"}, &values).Resolve()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
		assert.Equal(t, 0, ra)

		var v value
		_, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("primary", "bar")}, &v).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("bad table", func(t *testing.T) {
		var values []string
		_, err := txn.Exec(internal.List{Table: "bad"}, &values).Resolve()
		assert.NotNil(t, err)

		v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}
		_, err = txn.Exec(internal.Insert{Table: "bad", Value: v}).Resolve()
		assert.NotNil(t, err)

		_, err = txn.Exec(internal.Update{Table: "bad", Value: v, Filter: Ft().IdxEq("primary", "foo")}).Resolve()
		assert.NotNil(t, err)

		_, err = txn.Exec(internal.Remove{Table: "bad", Filter: Ft().IdxEq("primary", "foo")}).Resolve()
		assert.NotNil(t, err)

		_, err = txn.Exec(internal.Get{Table: "bad", Filter: Ft().IdxEq("primary", "foo")}, &v).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("bad insert", func(t *testing.T) {
		ra, err := txn.Exec(internal.Insert{Table: "tests"}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Insert{Table: "tests", Value: &map[string]interface{}{}}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Insert{Table: "tests", Value: map[string]interface{}{}}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("bad op", func(t *testing.T) {
		var values []string
		_, err := txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}, &values).Resolve()
		assert.NotNil(t, err)

		v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}
		_, err = txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("count", 2, true)}).Resolve()
		assert.NotNil(t, err)

		_, err = txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("count", 2, true)}).Resolve()
		assert.NotNil(t, err)

		_, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("count", 2, true)}, &v).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("can insert", func(t *testing.T) {
		ra, err := txn.Exec(internal.Insert{Table: "tests", Value: &value{
			Id:      "foo",
			Count:   1,
			Enabled: true,
		}}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})

	t.Run("unique constraint", func(t *testing.T) {
		_, err := txn.Exec(internal.Insert{Table: "tests", Value: &value{
			Id:      "foo",
			Count:   1,
			Enabled: true,
		}}).Resolve()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	_ = txn.Commit()
	t.Run("bad remove", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO(), true)
		ra, err := txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("id", "foo")}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("bad update", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		var v value
		ra, err := txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("id", "foo")}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	txn, _ = p.Txn(context.TODO())
	defer txn.Rollback()
	t.Run("can update", func(t *testing.T) {
		v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}
		ra, err := txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("primary", "foo")}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})

	t.Run("bad read", func(t *testing.T) {
		ra, err := txn.Exec(internal.List{Table: "tests"}, []value{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("id", "foo")}, &[]value{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.List{Table: "tests"}, &value{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.List{Table: "tests"}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		var v string
		_, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}, &v).Resolve()
		assert.NotNil(t, err)

		var values []string
		_, err = txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxEq("count", 2, true)}, &values).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("orphan clean-up", func(t *testing.T) {
		txn.Exec(internal.Insert{Table: "tests", Value: &value{
			Id:      "foo2",
			Count:   1,
			Enabled: true,
		}})

		txn.Exec(internal.Insert{Table: "tests", Value: &value{
			Id:      "foo3",
			Count:   1,
			Enabled: true,
		}})

		txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("primary", "foo2")})

		var values []value
		ra, err := txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxEq("count", 1, true)}, &values).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})

	t.Run("can list", func(t *testing.T) {
		var values []value
		ra, err := txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxEq("count", 2, true)}, &values).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Len(t, values, 1)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}, values[0])

		var v value
		ra, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}, &v).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0", Count: 2, Enabled: true}, v)
	})

	t.Run("can remove", func(t *testing.T) {
		ra, err := txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("primary", "foo")}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})
}
