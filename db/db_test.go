package db

import (
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	t.Parallel()

	opts := []Option{
		RDB(Schema{}),
		DSN(""),
		SQL(nil),
		SQLTrace(nil),
		SQLOpen(nil),
		DriverName(""),
		MaxConns(0),
		MaxIdleLifetime(0),
		MaxConnLifetime(0),
		Migration(nil, "", ""),
		Redis(redis.Options{}),
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

	opts := []CommandOption{
		TTL(0),
		CommandKey(""),
		CommandSQLPlaceholder(""),
	}
	cmd := CommandWith(opts)
	assert.NotEqual(t, Command{}, cmd)
}

func TestQueryOption(t *testing.T) {
	t.Parallel()

	opts := []QueryOption{
		QueryKey(""),
		QuerySQLPlaceholder(""),
		Eq("", "bar4"),
		NotEq("", ""),
		Fields("", ""),
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

	t.Run("slice present", func(t *testing.T) {
		query := QueryWith([]QueryOption{Fields("field1", []string{"field2"})})
		assert.Len(t, query.Fields, 1)
		assert.Equal(t, []string{"field2"}, query.Fields)
	})

	t.Run("mixed args", func(t *testing.T) {
		query := QueryWith([]QueryOption{Fields("field1", map[string]interface{}{"field3": ""})})
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
		assert.Contains(t, query.Fields, "Field2")
	})

	t.Run("unknown type", func(t *testing.T) {
		var v int
		query := QueryWith([]QueryOption{Fields(&v)})
		assert.Len(t, query.Fields, 0)
	})
}
