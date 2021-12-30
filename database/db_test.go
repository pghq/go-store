package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	t.Parallel()

	opts := []Option{
		Storage(Schema{}),
		SQLOpen(nil),
		Migrate(nil, "", ""),
	}
	config := ConfigWith(opts)
	assert.NotEqual(t, Config{}, config)
}

func TestTxnOption(t *testing.T) {
	t.Parallel()

	opts := []TxnOption{
		ReadOnly(),
		BatchWrite(),
		ViewTTL(0),
		BatchReadSize(0),
	}
	config := TxnConfigWith(opts)
	assert.NotEqual(t, TxnConfig{}, config)
}

func TestRequestOption(t *testing.T) {
	t.Parallel()

	opts := []RequestOption{
		Eq("", "bar4"),
		NotEq("", ""),
		As("", ""),
		Field(""),
		XEq("", ""),
		NotXEq("", ""),
		Page(0),
		Limit(0),
		OrderBy(""),
		GroupBy(""),
		Gt("", 0),
		Lt("", 0),
		Table(""),
		Filter(""),
		Suffix(""),
		Expire(0),
	}
	req := NewRequest(opts)
	assert.NotEqual(t, Request{}, req)
	assert.True(t, req.HasFilter())
}

func TestFields(t *testing.T) {
	t.Parallel()

	t.Run("field func", func(t *testing.T) {
		req := NewRequest([]RequestOption{
			Field([]string{"field2"}),
			As("field2", "field1"),
		})
		assert.Len(t, req.Fields, 1)
		assert.NotNil(t, req.Fields["field2"])
		assert.Equal(t, "field1", req.Fields["field2"].Format)
	})

	t.Run("slice present", func(t *testing.T) {
		req := NewRequest(Field("field1"), Field([]string{"field2"}))
		assert.Len(t, req.Fields, 1)
		assert.NotNil(t, req.Fields["field2"])
		assert.Equal(t, "field2", req.Fields["field2"].Format)
	})

	t.Run("mixed args", func(t *testing.T) {
		req := NewRequest([]RequestOption{Field("field1"), Field(map[string]interface{}{"field3": ""})})
		assert.Len(t, req.Fields, 2)
		assert.Contains(t, req.Fields, "field1")
		assert.Contains(t, req.Fields, "field3")
	})

	t.Run("struct", func(t *testing.T) {
		var v struct {
			Field1 int `db:"field1"`
			Field2 int
		}
		req := NewRequest([]RequestOption{Field(&v)})
		assert.Len(t, req.Fields, 2)
		assert.Contains(t, req.Fields, "field1")
		assert.Contains(t, req.Fields, "field2")
	})

	t.Run("unknown type", func(t *testing.T) {
		var v int
		req := NewRequest([]RequestOption{Field(&v)})
		assert.Len(t, req.Fields, 0)
	})
}
