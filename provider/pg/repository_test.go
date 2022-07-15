package pg

import (
	"context"
	"testing"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestRepository_Add(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	t.Run("bad data encode", func(t *testing.T) {
		assert.NotNil(t, repo.Add(context.TODO(), "tests", func() {}))
	})

	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.Add(context.TODO(), "", nil))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "1234"}))
	})
}

func TestRepository_All(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.All(context.TODO(), spec(""), nil))
	})

	t.Run("ok", func(t *testing.T) {
		var v []struct{ Id string }
		assert.Nil(t, repo.All(context.TODO(), spec("SELECT id FROM tests WHERE id = '1234'"), &v))
		assert.NotEmpty(t, v)
	})
}

func TestRepository_BatchQuery(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = db.Repository()
}

func TestRepository_Edit(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	t.Run("bad data encode", func(t *testing.T) {
		assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), func() {}))
	})

	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), nil))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, repo.Edit(context.TODO(), "tests", spec("id = 'edit:1234'"), map[string]interface{}{"id": "1234"}))
	})
}

func TestRepository_One(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.One(context.TODO(), spec(""), nil))
	})

	t.Run("ok", func(t *testing.T) {
		var v struct{ Id string }
		assert.Nil(t, repo.One(context.TODO(), spec("SELECT id FROM tests WHERE id = '1234'"), &v))
		assert.NotEqual(t, "", v.Id)
	})
}

func TestRepository_Remove(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.Remove(context.TODO(), "", spec("")))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, repo.Remove(context.TODO(), "tests", spec("id = 'edit:1234'")))
	})
}

type spec string

func (s spec) Id() interface{} {
	return string(s)
}

func (s spec) ToSql() (string, []interface{}, error) {
	if s == "" {
		return "", nil, trail.NewError("bad SQL statement")
	}

	return string(s), nil, nil
}
