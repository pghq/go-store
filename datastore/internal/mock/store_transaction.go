package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore"
)

var (
	_ datastore.Transaction = NewTransaction(nil)
)

func (s *Store) Transaction(ctx context.Context) (datastore.Transaction, error) {
	s.t.Helper()
	res := s.Call(s.t, ctx)
	if len(res) != 2 {
		s.fail(s.t, "unexpected length of return values")
		return nil, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			s.fail(s.t, "unexpected type of return value")
			return nil, nil
		}
		return nil, err
	}

	transaction, ok := res[0].(datastore.Transaction)
	if !ok {
		s.fail(s.t, "unexpected type of return value")
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

func (tx *Transaction) Execute(statement datastore.Encoder) (int, error) {
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

// NewTransactionWithFail creates a mock datastore.Transaction with an expected failure
func NewTransactionWithFail(t *testing.T, expect ...interface{}) *Transaction {
	tx := NewTransaction(t)
	tx.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return tx
}
