package database

import (
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	t.Parallel()

	opts := []Option{
		SQLOpen(nil),
		Migrate(nil),
		MigrateDirectory("", ""),
		SeedDirectory(""),
	}
	config := ConfigWith(opts)
	assert.Equal(t, Config{}, config)
}

func TestTxnOption(t *testing.T) {
	t.Parallel()

	opts := []TxnOption{
		ReadOnly(),
		BatchWrite(),
		ViewTTL(0),
	}
	config := TxnConfigWith(opts)
	assert.NotEqual(t, TxnConfig{}, config)
}

func TestQueryOption(t *testing.T) {
	t.Parallel()
	req := Query{Tables: []Expression{Expr("")}}
	assert.NotEqual(t, Query{}, req)
	assert.NotEqual(t, []byte{}, Query{}.Key(""))
}

func TestFields(t *testing.T) {
	t.Parallel()

	t.Run("slice present", func(t *testing.T) {
		fields := AppendFields(nil, "field1", []string{"field2"})
		assert.Len(t, fields, 1)
		assert.NotNil(t, fields[0])
		assert.Equal(t, "field2", fields[0])
	})

	t.Run("mixed args", func(t *testing.T) {
		fields := AppendFields(nil, "field1", map[string]interface{}{"field3": ""})
		assert.Len(t, fields, 2)
		assert.Contains(t, fields, "field1")
		assert.Contains(t, fields, "field3")
	})

	t.Run("struct", func(t *testing.T) {
		var v struct {
			Field1 int `db:"field1"`
			Field2 int
		}
		fields := AppendFields([]string{"field"}, &v)
		assert.Len(t, fields, 3)
		assert.Contains(t, fields, "field1")
		assert.Contains(t, fields, "field2")
	})

	t.Run("unknown type", func(t *testing.T) {
		var v int
		fields := AppendFields(nil, &v)
		assert.Len(t, fields, 0)
	})
}

func TestQuery_ToSql(t *testing.T) {
	query := Query{
		Format:   squirrel.Dollar,
		Fields:   []string{"tests.id"},
		XFields:  []Expression{Expr("tests.name")},
		Alias:    map[string]string{"tests.id": "tests.id"},
		NotEq:    map[string]interface{}{"name": "bar4"},
		XEq:      map[string]interface{}{"name": "%bar%"},
		Limit:    1,
		Page:     1,
		OrderBy:  []string{"name"},
		Table:    "tests",
		Eq:       map[string]interface{}{"id": "remove:foo"},
		Gt:       map[string]interface{}{"num": 0},
		Lt:       map[string]interface{}{"num": 2},
		Px:       map[string]string{"name": "bar"},
		Tables:   []Expression{Expr("LEFT JOIN units ON units.id = tests.id")},
		Filters:  []Expression{Expr("name = 'bar4'")},
		Suffixes: []Expression{Expr("UNION SELECT id, name FROM units WHERE units.id = ''")},
	}

	t.Run("ok", func(t *testing.T) {
		_, _, err := query.ToSql()
		assert.False(t, trail.IsFatal(err))
	})
}
