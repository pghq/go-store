package ark

import (
	"context"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal/mock"
)

func TestStore(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		client := mock.NewClient(t)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))
		assert.NotNil(t, r)
	})

	t.Run("can get fields", func(t *testing.T) {
		fields := Fields(map[string]interface{}{"key": ""}, "collection", []string{})
		assert.Equal(t, []string{"key", "collection"}, fields)

		fields = Fields(map[string]interface{}{"key": ""}, "collection", []string{"field"})
		assert.Equal(t, []string{"field"}, fields)
	})
}

func TestStore_Add(t *testing.T) {
	t.Run("raises nil item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", nil)
		assert.NotNil(t, err)
	})

	t.Run("raises transaction errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(nil, tea.NewError("an error has occurred"))
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		_, err := r.Context(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("raises execution errors", func(t *testing.T) {
		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectFailedTransactionExecute(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", map[string]interface{}{"key": 1337})
		assert.NotNil(t, err)
	})

	t.Run("raises commit errors", func(t *testing.T) {
		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectFailedTransactionCommit(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", map[string]interface{}{"key": 1337})
		assert.Nil(t, err)

		err = tx.Commit()
		assert.NotNil(t, err)
	})

	t.Run("raises bad item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", "item")
		assert.NotNil(t, err)
	})

	t.Run("raises bad slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := NewStore("", RawClient(client))
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", []string{"item"})
		assert.NotNil(t, err)
	})

	t.Run("raises empty slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := NewStore("", RawClient(client))
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", []interface{}{})
		assert.NotNil(t, err)
	})

	t.Run("can add one", func(t *testing.T) {
		item := map[string]interface{}{
			"key": 1337,
		}

		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", item).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransaction(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		var is struct {
			Key       int    `db:"key"`
			Transient string `db:"value,transient"`
			Ignore    string `db:"-"`
		}
		is.Key = 1337
		is.Transient = "transient"
		is.Ignore = "ignore"
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", &is)
		assert.Nil(t, err)

		tx, _ = r.Context(tx)
		defer tx.Rollback()
		defer tx.Commit()
		assert.NotNil(t, tx)
	})

	t.Run("can add many", func(t *testing.T) {
		items := []map[string]interface{}{{"key": 1337}, {"key": 1337}}
		add := mock.NewAdd(t)
		for _, item := range items {
			add.Expect("To", "tests").
				Return(add)
			add.Expect("Item", item).
				Return(add)
		}
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransactions(t, add, add), nil)
		for range items {
			client.Expect("Add").
				Return(add)
		}
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		var is struct {
			Key int `db:"key"`
		}
		is.Key = 1337
		is2 := is
		is2.Key = 1337
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", &is)
		assert.Nil(t, err)
		err = tx.Add("tests", &is2)
		assert.Nil(t, err)
	})
}

func TestStore_Search(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Query").
			Return(mock.NewQuery(t))
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))
		assert.NotNil(t, r.Query())
	})

	t.Run("raises execution errors", func(t *testing.T) {
		var dst []map[string]interface{}
		query := mock.NewQuery(t)
		query.Expect("String").
			Return("raises execution errors")
		defer query.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectFailedTransactionExecute(t, query, &dst), nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Search(query, &dst, Consistent())
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		var dst []map[string]interface{}
		query := mock.NewQuery(t)
		query.Expect("String").
			Return("can execute")
		query.Expect("String").
			Return("can execute")
		defer query.Assert(t)

		txn := mock.NewTransaction(t)
		txn.Expect("Execute", append([]interface{}{query}, &dst)...).
			Return(0, tea.NewNoContent("not found"))
		txn.Expect("Rollback").
			Return(nil)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(txn, nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		cached, err := tx.Search(query, &dst)
		assert.NotNil(t, err)
		assert.False(t, cached)

		r.cache.Wait()
		cached, err = tx.Search(query, &dst)
		assert.NotNil(t, err)
		assert.True(t, cached)
	})

	t.Run("transaction errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(nil, tea.NewError("an error has occurred"))
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))
		_, err := r.Get(context.TODO(), nil, nil)
		assert.NotNil(t, err)
	})

	t.Run("can execute procedure", func(t *testing.T) {
		var dst []map[string]interface{}
		query := mock.NewQuery(t)
		query.Expect("String").
			Return("can execute")
		query.Expect("String").
			Return("can execute")
		defer query.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransaction(t, query, &dst), nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		cached, err := tx.Search(query, &dst)
		assert.Nil(t, err)
		assert.False(t, cached)

		r.cache.Wait()
		cached, err = tx.Search(query, &dst)
		assert.Nil(t, err)
		assert.True(t, cached)
	})

	t.Run("can execute search", func(t *testing.T) {
		var dst []map[string]interface{}
		query := mock.NewQuery(t)
		query.Expect("String").
			Return("can execute")
		defer query.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), true).
			Return(expectTransaction(t, query, &dst), nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))
		_, err := r.Get(context.TODO(), query, &dst, ReadOnly())
		assert.Nil(t, err)
	})
}

func TestStore_Remove(t *testing.T) {
	t.Run("raises execution errors", func(t *testing.T) {
		remove := expectRemove(t)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectFailedTransactionExecute(t, remove), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Remove("tests", nil)
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		remove := expectRemove(t)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransaction(t, remove), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Remove("tests", nil)
		assert.Nil(t, err)
	})
}

func TestStore_Update(t *testing.T) {
	t.Run("raises bad item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).Return(expectFailedTransaction(t), nil)
		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, nil)
		assert.NotNil(t, err)
	})

	t.Run("raises execution errors", func(t *testing.T) {
		update := expectUpdate(t, map[string]interface{}{"key": 1337})
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectFailedTransactionExecute(t, update), nil)
		client.Expect("Update").
			Return(update)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337})
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		update := expectUpdate(t, map[string]interface{}{"key": 1337})
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Update").
			Return(update)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransaction(t, update), nil)
		defer client.Assert(t)

		r, _ := NewStore("", RawClient(client))

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337})
		assert.Nil(t, err)
	})
}

func TestStore_Context(t *testing.T) {
	t.Run("should notify on procedure errors", func(t *testing.T) {
		t.Run("new transaction failed", func(t *testing.T) {
			client := mock.NewClient(t)
			client.Expect("Transaction", context.TODO(), false).
				Return(nil, tea.NewError("an error has occurred"))

			r, _ := NewStore("", RawClient(client))
			err := r.Do(context.TODO())
			assert.NotNil(t, err)
		})

		t.Run("subroutine failed", func(t *testing.T) {
			client := mock.NewClient(t)
			client.Expect("Transaction", context.TODO(), false).
				Return(expectTransactions(t), nil)

			r, _ := NewStore("", RawClient(client))
			err := r.Do(context.TODO(), func(tx *Context) error {
				return tea.NewError("an error")
			}, func(tx *Context) error {
				return tea.NewError("another error")
			})

			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "an error")
		})
	})

	t.Run("should notify on success", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO(), false).
			Return(expectTransactions(t), nil)

		r, _ := NewStore("", RawClient(client))
		err := r.Do(context.TODO(), func(tx *Context) error {
			return nil
		}, func(tx *Context) error {
			return nil
		})

		assert.Nil(t, err)
	})
}

func expectTransaction(t *testing.T, command interface{}, dst ...interface{}) *mock.Transaction {
	transaction := mock.NewTransaction(t)

	transaction.Expect("Execute", append([]interface{}{command}, dst...)...).
		Return(0, nil)

	transaction.Expect("Commit").
		Return(nil)
	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectTransactions(t *testing.T, commands ...interface{}) *mock.Transaction {
	transaction := mock.NewTransaction(t)

	for _, command := range commands {
		transaction.Expect("Execute", command).
			Return(0, nil)
	}

	transaction.Expect("Commit").
		Return(nil)
	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectFailedTransaction(t *testing.T) *mock.Transaction {
	transaction := mock.NewTransaction(t)

	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectFailedTransactionCommit(t *testing.T, commands ...interface{}) *mock.Transaction {
	transaction := mock.NewTransaction(t)

	for _, command := range commands {
		transaction.Expect("Execute", command).
			Return(0, nil)
	}

	transaction.Expect("Commit").
		Return(tea.NewError("an error has occurred"))
	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectFailedTransactionExecute(t *testing.T, command interface{}, dst ...interface{}) *mock.Transaction {
	transaction := mock.NewTransaction(t)

	transaction.Expect("Execute", append([]interface{}{command}, dst...)...).
		Return(0, tea.NewError("an error has occurred"))

	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectRemove(t *testing.T) *mock.Remove {
	remove := mock.NewRemove(t)
	remove.Expect("From", "tests").
		Return(remove)
	remove.Expect("Filter", nil).
		Return(remove)
	return remove
}

func expectUpdate(t *testing.T, item map[string]interface{}) *mock.Update {
	update := mock.NewUpdate(t)
	update.Expect("In", "tests").
		Return(update)
	update.Expect("Filter", nil).
		Return(update)
	update.Expect("Item", item).
		Return(update)

	return update
}
