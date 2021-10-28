package mock

import (
	"context"
	"testing"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Transaction = NewTransaction(nil)
)

func (c *Client) Transaction(ctx context.Context) (client.Transaction, error) {
	c.t.Helper()
	res := c.Call(c.t, ctx)
	if len(res) != 2 {
		c.fail(c.t, "unexpected length of return values")
		return nil, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			c.fail(c.t, "unexpected type of return value")
			return nil, nil
		}
		return nil, err
	}

	transaction, ok := res[0].(client.Transaction)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return nil, nil
	}

	return transaction, nil
}

// Transaction is a mock datastore.Transaction
type Transaction struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (tx *Transaction) Commit() error {
	tx.t.Helper()
	res := tx.Call(tx.t)
	if len(res) != 1 {
		tx.fail(tx.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			tx.fail(tx.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

func (tx *Transaction) Rollback() error {
	tx.t.Helper()
	res := tx.Call(tx.t)
	if len(res) != 1 {
		tx.fail(tx.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			tx.fail(tx.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

func (tx *Transaction) Execute(statement client.Encoder) (int, error) {
	tx.t.Helper()
	res := tx.Call(tx.t, statement)
	if len(res) != 2 {
		tx.fail(tx.t, "unexpected length of return values")
		return 0, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			tx.fail(tx.t, "unexpected type of return value")
			return 0, nil
		}
		return 0, err
	}

	count, ok := res[0].(int)
	if !ok {
		tx.fail(tx.t, "unexpected type of return value")
		return 0, nil
	}

	return count, nil
}

// NewTransaction creates a mock datastore.Transaction
func NewTransaction(t *testing.T) *Transaction {
	tx := Transaction{
		t: t,
	}

	if t != nil {
		tx.fail = t.Fatal
	}

	return &tx
}
