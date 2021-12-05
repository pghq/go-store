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

	t.Run("bad schema", func(t *testing.T) {
		p := NewProvider("{}")
		assert.NotNil(t, p.Connect(context.TODO()))
	})

	t.Run("one index", func(t *testing.T) {
		p := NewProvider(`{
			"tests": {
				"id": {
					"unique": true, 
					"fields": {
						"Id": "string"
					}
				}
			}
		}`)
		assert.Nil(t, p.Connect(context.TODO()))
	})

	t.Run("multiple indexes", func(t *testing.T) {
		p := NewProvider(`{
			"tests": {
				"id": {
					"unique": true, 
					"fields": {
						"Id": "string"
					}
				},
				"failures": {
					"fields": {
						"Id": "string"
					}
				}
			}
		}`)
		assert.Nil(t, p.Connect(context.TODO()))
	})

	t.Run("multiple tables", func(t *testing.T) {
		p := NewProvider(`{
			"tests": {
				"id": {
					"unique": true, 
					"fields": {
						"Id": "string"
					}
				}
			},
			"units": {
				"id": {
					"unique": true, 
					"fields": {
						"Id": "string"
					}
				}
			}
		}`)
		assert.Nil(t, p.Connect(context.TODO()))
	})
}

func TestRDB_Txn(t *testing.T) {
	type value struct {
		Id        string
		Latitude  string
		Longitude string
		Count     int
		Enabled   bool
	}

	p := NewProvider(`{
		"tests": {
			"id": {
				"unique": true, 
				"fields": {
					"Id": "string"
				}
			},
			"point": {
				"fields": {
					"Latitude": "string",
					"Longitude": "string"
				}
			},
			"count": {
				"fields": {
					"Count": "int"
				}
			},
			"enabled": {
				"fields": {
					"Enabled": "bool"
				}
			}
		}
	}`)
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

	t.Run("not found", func(t *testing.T) {
		ra, err := txn.Exec(internal.List{Table: "tests"}, nil).Resolve()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
		assert.Equal(t, 0, ra)
	})

	t.Run("bad insert", func(t *testing.T) {
		ra, err := txn.Exec(internal.Insert{Table: "tests"}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.Insert{Table: "tests", Value: &map[string]interface{}{}}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		var v value
		ra, err = txn.Exec(internal.Insert{Table: "tests", Value: v}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
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
		v := value{Id: "foo", Latitude: "1.0", Longitude: "1.0"}
		ra, err := txn.Exec(internal.Update{Table: "tests", Value: v, Filter: Ft().IdxEq("id", "foo")}).Resolve()
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
	})

	t.Run("can read all", func(t *testing.T) {
		var values []value
		ra, err := txn.Exec(internal.List{Table: "tests"}, &values).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Len(t, values, 1)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0"}, values[0])
	})

	t.Run("can read filter", func(t *testing.T) {
		var values []value
		ra, err := txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxEq("id", "foo")}, &values).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Len(t, values, 1)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0"}, values[0])

		var v value
		ra, err = txn.Exec(internal.Get{Table: "tests", Filter: Ft().IdxEq("id", "foo")}, &v).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0"}, v)

		values = []value{}
		ra, err = txn.Exec(internal.List{Table: "tests", Filter: Ft().IdxBeginsWith("id", "f")}, &values).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Len(t, values, 1)
		assert.Equal(t, value{Id: "foo", Latitude: "1.0", Longitude: "1.0"}, values[0])
	})

	t.Run("can remove", func(t *testing.T) {
		ra, err := txn.Exec(internal.Remove{Table: "tests", Filter: Ft().IdxEq("id", "foo")}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})
}
