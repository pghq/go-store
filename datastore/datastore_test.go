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
		defer tx.Rollback()
		err := tx.Add("tests", nil)
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
		add := mock.NewAdd(t)
		add.Expect("To", "tests").
			Return(add)
		add.Expect("Item", map[string]interface{}{"key": 1337}).
			Return(add)
		defer add.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := New(client)

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
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionCommit(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", map[string]interface{}{"key": 1337})
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
		defer tx.Rollback()
		err := tx.Add("tests", "item")
		assert.NotNil(t, err)
	})

	t.Run("raises bad slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := New(client)
		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		err := tx.Add("tests", []string{"item"})
		assert.NotNil(t, err)
	})

	t.Run("raises empty slice errors", func(t *testing.T) {
		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).Return(expectFailedTransaction(t), nil)
		defer client.Assert(t)
		r, _ := New(client)
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
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, add), nil)
		client.Expect("Add").
			Return(add)
		defer client.Assert(t)

		r, _ := New(client)

		var is struct {Key int `db:"key"`}
		is.Key = 1337
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
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, add, add), nil)
		for range items {
			client.Expect("Add").
				Return(add)
		}
		defer client.Assert(t)

		r, _ := New(client)

		var is struct {Key int `db:"key"`}
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
		remove := expectRemove(t)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, remove), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Remove("tests", nil, 1)
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		remove := expectRemove(t)
		defer remove.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, remove), nil)
		client.Expect("Remove").
			Return(remove)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
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
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, nil, 1)
		assert.NotNil(t, err)
	})

	t.Run("raises execution errors", func(t *testing.T) {
		update := expectUpdate(t, map[string]interface{}{"key": 1337})
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Transaction", context.TODO()).
			Return(expectFailedTransactionExecute(t, update), nil)
		client.Expect("Update").
			Return(update)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337}, 1)
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		update := expectUpdate(t, map[string]interface{}{"key": 1337})
		defer update.Assert(t)

		client := mock.NewClient(t)
		client.Expect("Update").
			Return(update)
		client.Expect("Transaction", context.TODO()).
			Return(expectTransaction(t, update), nil)
		defer client.Assert(t)

		r, _ := New(client)

		tx, _ := r.Context(context.TODO())
		defer tx.Rollback()
		_, err := tx.Update("tests", nil, map[string]interface{}{"key": 1337}, 1)
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
	transaction.Expect("Rollback").
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

func expectRemove(t *testing.T) *mock.Remove{
	remove := mock.NewRemove(t)
	remove.Expect("From", "tests").
		Return(remove)
	remove.Expect("Filter", nil).
		Return(remove)
	remove.Expect("First", 1).
		Return(remove)
	return remove
}

func expectUpdate(t *testing.T, item map[string]interface{}) *mock.Update{
	update := mock.NewUpdate(t)
	update.Expect("In", "tests").
		Return(update)
	update.Expect("Filter", nil).
		Return(update)
	update.Expect("First", 1).
		Return(update)
	update.Expect("Item", item).
		Return(update)

	return update
}
