package datastore

import (
	"context"
	"testing"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore/internal/mock"
)

func TestRepository(t *testing.T) {
	t.Run("raises error on undefined client", func(t *testing.T) {
		_, err := New(nil)
		assert.NotNil(t, err)
	})

	t.Run("raises error on disconnected client", func(t *testing.T) {
		client := mock.NewDisconnectedClient(t)
		client.Expect("Connect").Return(errors.New("an error has occurred"))
		defer client.Assert(t)

		_, err := New(client)
		assert.NotNil(t, err)
	})

	t.Run("can create instance", func(t *testing.T) {
		client := mock.NewClient(t)
		defer client.Assert(t)

		r, err := New(client)
		assert.Nil(t, err)
		assert.NotNil(t, r)
	})
}

func TestRepository_Add(t *testing.T) {
	t.Run("raises nil item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)

		r, _ := New(client)
		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", nil)
		assert.NotNil(t, err)
	})

	t.Run("raises transaction errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(nil, errors.New("an error has occurred"))
		defer client.Assert(t)

		r, _ := New(client)

		_, err := r.Context(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("raises execution errors", func(t *testing.T) {
		expect := mock.NewAdd(t)
		expect.Expect("To", "tests").
			Return(expect)
		expect.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(expect)

		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, expect), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", map[string]interface{}{"key": 1337})
		assert.NotNil(t, err)
	})

	t.Run("raises commit errors", func(t *testing.T) {
		expect := mock.NewAdd(t)
		expect.Expect("To", "tests").
			Return(expect)
		expect.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(expect)

		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionCommit(t, expect), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", map[string]interface{}{"key": 1337})
		assert.Nil(t, err)

		err = tx.Commit()
		assert.NotNil(t, err)
	})

	t.Run("raises bad item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", "item")
		assert.NotNil(t, err)
	})

	t.Run("raises bad slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := New(client)
		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", []string{"item"})
		assert.NotNil(t, err)
	})

	t.Run("raises empty slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := New(client)
		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", []interface{}{})
		assert.NotNil(t, err)
	})

	t.Run("can add one", func(t *testing.T) {
		items := []map[string]interface{}{
			{"key": 1337},
		}

		execute := mock.NewAdd(t)
		for _, item := range items {
			execute.Expect("To", "tests").
				Return(execute)
			execute.Expect("Item", item).
				Return(execute)
		}

		add := mock.NewAdd(t)
		for _, item := range items {
			add.Expect("To", "tests").
				Return(add)
			add.Expect("Item", item).
				Return(add)
		}
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, execute), nil)
		for range items {
			client.Expect("Add").
				Return(add)
		}
		defer client.Assert(t)

		r, _ := New(client)

		var item struct {
			Key int `db:"key"`
		}

		item.Key = 1337

		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", &item)
		assert.Nil(t, err)

		tx, _ = r.Context(tx)
		assert.NotNil(t, tx)
	})

	t.Run("can add many", func(t *testing.T) {
		items := []map[string]interface{}{
			{"key": 1337},
			{"key": 1337},
		}

		execute := mock.NewAdd(t)
		for _, item := range items {
			execute.Expect("To", "tests").
				Return(execute)
			execute.Expect("Item", item).
				Return(execute)
		}

		add := mock.NewAdd(t)
		for _, item := range items {
			add.Expect("To", "tests").
				Return(add)
			add.Expect("Item", item).
				Return(add)
		}
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, execute, execute), nil)
		for range items {
			client.Expect("Add").
				Return(add)
		}
		defer client.Assert(t)

		r, _ := New(client)

		var item struct {
			Key int `db:"key"`
		}

		item.Key = 1337

		var item2 struct {
			Key int `db:"key"`
		}

		item2.Key = 1337
		tx, _ := r.Context(context.TODO())
		_, err := tx.Add("tests", &item)
		assert.Nil(t, err)
		_, err = tx.Add("tests", &item2)
		assert.Nil(t, err)
	})
}

func TestRepository_Search(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Query").
			Return(mock.NewQuery(t))
		defer client.Assert(t)

		r, _ := New(client)
		assert.NotNil(t, r.Query())
	})

	t.Run("can execute", func(t *testing.T) {
		var dst []map[string]interface{}
		query := mock.NewQuery(t)
		query.Expect("Execute", context.TODO(), &dst).
			Return(nil)
		defer query.Assert(t)

		client := mock.NewClient(t)
		defer client.Assert(t)

		r, _ := New(client)

		err := r.Search(context.TODO(), query, &dst)
		assert.Nil(t, err)
	})
}

func TestRepository_Remove(t *testing.T) {
	t.Run("raises execution errors", func(t *testing.T) {
		expect := mock.NewRemove(t)
		expect.Expect("From", "tests").
			Return(expect)
		expect.Expect("Filter", nil).
			Return(expect)
		expect.Expect("First", 1).
			Return(expect)

		remove := mock.NewRemove(t)
		remove.Expect("From", "tests").
			Return(remove)
		remove.Expect("Filter", nil).
			Return(remove)
		remove.Expect("First", 1).
			Return(remove)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, expect), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Remove("tests", nil, 1)
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		remove := mock.NewRemove(t)
		remove.Expect("From", "tests").
			Return(remove)
		remove.Expect("Filter", nil).
			Return(remove)
		remove.Expect("First", 1).
			Return(remove)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, remove), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Remove("tests", nil, 1)
		assert.Nil(t, err)
	})
}

func TestRepository_Filter(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Filter").
			Return(mock.NewFilter(t))
		defer client.Assert(t)

		r, _ := New(client)
		assert.NotNil(t, r.Filter())
	})
}

func TestRepository_Update(t *testing.T) {
	t.Run("raises bad item errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Update("tests", nil, nil)
		assert.NotNil(t, err)
	})

	t.Run("raises execution errors", func(t *testing.T) {
		expect := mock.NewUpdate(t)
		expect.Expect("In", "tests").
			Return(expect)
		expect.Expect("Filter", nil).
			Return(expect)
		expect.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(expect)

		update := mock.NewUpdate(t)
		update.Expect("In", "tests").
			Return(update)
		update.Expect("Filter", nil).
			Return(update)
		update.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(update)
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, expect), nil)
		client.Expect("Update").
			Return(update)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337})
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		update := mock.NewUpdate(t)
		update.Expect("In", "tests").
			Return(update)
		update.Expect("Filter", nil).
			Return(update)
		update.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(update)
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Update").
			Return(update)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, update), nil)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337})
		assert.Nil(t, err)
	})
}

func expectTransaction(t *testing.T, commands ...interface{}) *mock.Transaction{
	transaction := mock.NewTransaction(t)

	for _, command := range commands{
		transaction.Expect("Execute", command).
			Return(0, nil)
	}

	transaction.Expect("Commit").
		Return(nil)

	return transaction
}

func expectFailedTransaction(t *testing.T) *mock.Transaction{
	transaction := mock.NewTransaction(t)

	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectFailedTransactionCommit(t *testing.T, commands ...interface{}) *mock.Transaction{
	transaction := mock.NewTransaction(t)

	for _, command := range commands{
		transaction.Expect("Execute", command).
			Return(0, nil)
	}

	transaction.Expect("Commit").
		Return(errors.New("an error has occurred"))
	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}

func expectFailedTransactionExecute(t *testing.T, command interface{}) *mock.Transaction{
	transaction := mock.NewTransaction(t)

	transaction.Expect("Execute", command).
		Return(0, errors.New("an error has occurred"))

	transaction.Expect("Rollback").
		Return(nil)

	return transaction
}
