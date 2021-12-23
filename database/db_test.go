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

func TestCommandOption(t *testing.T) {
	t.Parallel()

	opts := []CommandOption{Expire(0)}
	cmd := CommandWith(opts)
	assert.NotEqual(t, Command{}, cmd)
}

func TestQueryOption(t *testing.T) {
	t.Parallel()

	opts := []QueryOption{
		Eq("", "bar4"),
		NotEq("", ""),
		Fields(""),
		XEq("", ""),
		NotXEq("", ""),
		Page(0),
		Limit(0),
		OrderBy(""),
		Gt("", 0),
		Lt("", 0),
		Table(""),
		Filter(""),
	}
	query := QueryWith(opts)
	assert.NotEqual(t, Query{}, query)
	assert.True(t, query.HasFilter())
}

func TestFields(t *testing.T) {
	t.Parallel()

	t.Run("field func", func(t *testing.T) {
		query := QueryWith([]QueryOption{Fields("field2", func(string) string { return "field1" }), Fields([]string{"field2"})})
		assert.Len(t, query.Fields, 1)
		assert.NotNil(t, query.Fields["field2"])
		assert.Equal(t, "field1", query.Fields["field2"]())
	})

	t.Run("slice present", func(t *testing.T) {
		query := QueryWith([]QueryOption{Fields("field1"), Fields([]string{"field2"})})
		assert.Len(t, query.Fields, 1)
		assert.NotNil(t, query.Fields["field2"])
		assert.Equal(t, "field2", query.Fields["field2"]())
	})

	t.Run("mixed args", func(t *testing.T) {
		query := QueryWith([]QueryOption{Fields("field1"), Fields(map[string]interface{}{"field3": ""})})
		assert.Len(t, query.Fields, 2)
		assert.Contains(t, query.Fields, "field1")
		assert.Contains(t, query.Fields, "field3")
	})

	t.Run("struct", func(t *testing.T) {
		var v struct {
			Field1 int `db:"field1"`
			Field2 int
		}
		query := QueryWith([]QueryOption{Fields(&v)})
		assert.Len(t, query.Fields, 2)
		assert.Contains(t, query.Fields, "field1")
		assert.Contains(t, query.Fields, "field2")
	})

	t.Run("unknown type", func(t *testing.T) {
		var v int
		query := QueryWith([]QueryOption{Fields(&v)})
		assert.Len(t, query.Fields, 0)
	})
}

func TestKey_String(t *testing.T) {
	t.Run("named key", func(t *testing.T) {
		assert.Equal(t, "test", NamedKey("foo", "test").String())
	})

	t.Run("id key", func(t *testing.T) {
		assert.Equal(t, "foo", Id("foo").String())
	})
}
