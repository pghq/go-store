package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
)

func TestSQLPlaceholderPrefix_ReplacePlaceholders(t *testing.T) {
	s := SQLPlaceholderPrefix("$")

	t.Run("escape", func(t *testing.T) {
		r, err := s.ReplacePlaceholders("SELECT * FROM tests WHERE name = ??")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT * FROM tests WHERE name = ?", r)
	})
}

func TestGet(t *testing.T) {
	t.Parallel()

	g := Get{
		Table:  "tests",
		Key:    []byte("test"),
		Filter: squirrel.Eq{"tests.id": 1},
		Fields: []interface{}{"id"},
		FieldFunc: func(s string) string {
			return fmt.Sprintf("tests.%s", s)
		},
	}

	t.Run("sql", func(t *testing.T) {
		s, args, err := g.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT tests.id FROM tests WHERE tests.id = $1 LIMIT 1", s)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("standard method", func(t *testing.T) {
		assert.Equal(t, StandardMethod{
			Get:    true,
			Table:  g.Table,
			Key:    g.Key,
			Filter: g.Filter,
		}, g.StandardMethod())
	})

	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, "get:tests:[id]:map[tests.id:1]:test", string(g.Bytes()))
	})
}

func TestInsert(t *testing.T) {
	t.Parallel()

	type value struct {
		Id     int `db:"id"`
		Ignore int `db:"ignore,transient"`
	}

	v := value{Id: 1, Ignore: 1}
	i := Insert{
		Table: "tests",
		Key:   []byte("test"),
		Value: &v,
	}

	t.Run("sql", func(t *testing.T) {
		t.Run("bad type", func(t *testing.T) {
			v := func() {}
			i := i
			i.Value = &v
			_, _, err := i.SQL("$")
			assert.NotNil(t, err)
		})

		s, args, err := i.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "INSERT INTO tests (id) VALUES ($1)", s)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("standard method", func(t *testing.T) {
		assert.Equal(t, StandardMethod{
			Insert: true,
			Table:  i.Table,
			Key:    i.Key,
			Value:  i.Value,
		}, i.StandardMethod())
	})

	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, "insert:tests:&{1 1}:test", string(i.Bytes()))
	})
}

func TestList(t *testing.T) {
	t.Parallel()

	now := time.Now()
	l := List{
		Table:     "tests",
		LeftJoins: []LeftJoin{{Join: "units ON units.id = ?", Args: []interface{}{1}}},
		Key:       []byte("test"),
		After:     After{Key: "tests.created_at", Value: &now},
		Filter:    squirrel.Eq{"tests.id": 1},
		Fields:    []interface{}{"id"},
		OrderBy:   []string{"created_at"},
		FieldFunc: func(s string) string {
			return fmt.Sprintf("tests.%s", s)
		},
		Limit: 1,
	}

	t.Run("sql", func(t *testing.T) {
		s, args, err := l.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT tests.id FROM tests LEFT JOIN units ON units.id = $1 WHERE tests.created_at > $2 AND tests.id = $3 ORDER BY created_at LIMIT 1", s)
		assert.Equal(t, []interface{}{1, &now, 1}, args)
	})

	t.Run("standard method", func(t *testing.T) {
		assert.Equal(t, StandardMethod{
			List:   true,
			Table:  l.Table,
			Key:    l.Key,
			Filter: l.Filter,
		}, l.StandardMethod())
	})

	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, fmt.Sprintf("list:tests:[{units ON units.id = ? [1]}]:[id]:map[tests.id:1]:{tests.created_at %s}:[created_at]:1:test", now), string(l.Bytes()))
	})
}

func TestRemove(t *testing.T) {
	t.Parallel()

	r := Remove{
		Table:   "tests",
		Key:     []byte("test"),
		Filter:  squirrel.Eq{"tests.id": 1},
		OrderBy: []string{"created_at"},
	}

	t.Run("sql", func(t *testing.T) {
		s, args, err := r.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "DELETE FROM tests WHERE tests.id = $1 ORDER BY created_at", s)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("standard method", func(t *testing.T) {
		assert.Equal(t, StandardMethod{
			Remove: true,
			Table:  r.Table,
			Key:    r.Key,
			Filter: r.Filter,
		}, r.StandardMethod())
	})

	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, "remove:tests:map[tests.id:1]:[created_at]:test", string(r.Bytes()))
	})
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	type value struct {
		Id     int `db:"id"`
		Ignore int `db:"ignore,transient"`
	}

	v := value{Id: 1, Ignore: 1}
	u := Update{
		Table:  "tests",
		Key:    []byte("test"),
		Filter: squirrel.Eq{"id": 1},
		Value:  &v,
	}

	t.Run("sql", func(t *testing.T) {
		t.Run("bad type", func(t *testing.T) {
			v := func() {}
			u := u
			u.Value = &v
			_, _, err := u.SQL("$")
			assert.NotNil(t, err)
		})

		s, args, err := u.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "UPDATE tests SET id = $1 WHERE id = $2", s)
		assert.Equal(t, []interface{}{1, 1}, args)
	})

	t.Run("standard method", func(t *testing.T) {
		assert.Equal(t, StandardMethod{
			Update: true,
			Table:  u.Table,
			Key:    u.Key,
			Value:  u.Value,
			Filter: u.Filter,
		}, u.StandardMethod())
	})

	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, "update:tests:map[id:1]:&{1 1}:test", string(u.Bytes()))
	})
}
