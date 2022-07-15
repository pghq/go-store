package pg

import (
	"context"
	"testing"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-store/provider"
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

	t.Run("unique violation error", func(t *testing.T) {
		err := repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "1234"})
		assert.NotNil(t, err)
		assert.True(t, trail.IsConflict(err))
	})
}

func TestRepository_All(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "all:1234"})
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.All(context.TODO(), spec(""), nil))
	})

	t.Run("ok", func(t *testing.T) {
		var v []struct{ Id string }
		assert.Nil(t, repo.All(context.TODO(), spec("SELECT id FROM tests WHERE id = 'all:1234'"), &v))
		assert.NotEmpty(t, v)
	})
}

func TestRepository_BatchQuery(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "batch.query:1234"})

	t.Run("bad sql", func(t *testing.T) {
		batch := provider.BatchQuery{}
		batch.One(spec(""), nil)
		assert.NotNil(t, repo.BatchQuery(context.TODO(), batch))
	})

	t.Run("not found", func(t *testing.T) {
		batch := provider.BatchQuery{}
		var one struct{ Id string }
		batch.One(spec("SELECT id FROM tests WHERE id = 'batch.query:foo'"), &one)
		assert.NotNil(t, repo.BatchQuery(context.TODO(), batch))
	})

	t.Run("ok", func(t *testing.T) {
		batch := provider.BatchQuery{}
		var all []struct{ Id string }
		batch.All(spec("SELECT id FROM tests WHERE id = 'batch.query:1234'"), &all)

		var one struct{ Id string }
		batch.One(spec("SELECT id FROM tests WHERE id = 'batch.query:1234'"), &one)

		assert.Nil(t, repo.BatchQuery(context.TODO(), batch))
		assert.NotEmpty(t, all)
		assert.NotEqual(t, "", one.Id)
	})
}

func TestRepository_Edit(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "edit:1234"})
	t.Run("bad data encode", func(t *testing.T) {
		assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), func() {}))
	})

	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), nil))
	})

	t.Run("unique violation error", func(t *testing.T) {
		assert.Nil(t, repo.Edit(context.TODO(), "tests", spec("id = 'edit:1234'"), map[string]interface{}{"id": "edit:1234"}))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, repo.Edit(context.TODO(), "tests", spec("id = 'edit:1234'"), map[string]interface{}{"id": "edit:1234"}))
	})

	t.Run("unique violation error", func(t *testing.T) {
		_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "edit:12345"})
		err := repo.Edit(context.TODO(), "tests", spec("id = 'edit:12345'"), map[string]interface{}{"id": "edit:1234"})
		assert.NotNil(t, err)
		assert.True(t, trail.IsConflict(err))
	})
}

func TestRepository_One(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "one:1234"})
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.One(context.TODO(), spec(""), nil))
	})

	t.Run("not found error", func(t *testing.T) {
		var v struct{ Id string }
		err := repo.One(context.TODO(), spec("SELECT id FROM tests WHERE id = 'one:foo'"), &v)
		assert.NotNil(t, err)
		assert.True(t, trail.IsNotFound(err))
	})

	t.Run("ok", func(t *testing.T) {
		var v struct{ Id string }
		assert.Nil(t, repo.One(context.TODO(), spec("SELECT id FROM tests WHERE id = 'one:1234'"), &v))
		assert.NotEqual(t, "", v.Id)
	})
}

func TestRepository_Remove(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := db.Repository()
	_ = repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "remove:1234"})
	t.Run("bad sql", func(t *testing.T) {
		assert.NotNil(t, repo.Remove(context.TODO(), "", spec("")))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, repo.Remove(context.TODO(), "tests", spec("id = 'remove:1234'")))
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
