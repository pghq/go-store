package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Add = NewAdd(nil)
)

func (c *Client) Add() client.Add {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	add, ok := res[0].(client.Add)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return nil
	}

	return add
}

// Add is a mock datastore.Add
type Add struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (a *Add) Query(query client.Query) client.Add {
	a.t.Helper()
	res := a.Call(a.t, query)
	if len(res) != 1 {
		a.fail(a.t, "unexpected length of return values")
		return nil
	}

	add, ok := res[0].(client.Add)
	if !ok {
		a.fail(a.t, "unexpected type of return value")
		return nil
	}

	return add
}

func (a *Add) Execute(ctx context.Context) (int, error) {
	a.t.Helper()
	res := a.Call(a.t, ctx)
	if len(res) != 2 {
		a.fail(a.t, "unexpected length of return values")
		return 0, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			a.fail(a.t, "unexpected type of return value")
			return 0, nil
		}
		return 0, err
	}

	count, ok := res[0].(int)
	if !ok {
		a.fail(a.t, "unexpected type of return value")
		return 0, nil
	}

	return count, nil
}

func (a *Add) Statement() (string, []interface{}, error) {
	a.t.Helper()
	res := a.Call(a.t)
	if len(res) != 3 {
		a.fail(a.t, "unexpected length of return values")
		return "", nil, nil
	}

	if res[2] != nil {
		err, ok := res[2].(error)
		if !ok {
			a.fail(a.t, "unexpected type of return value")
			return "", nil, nil
		}
		return "", nil, err
	}

	statement, ok := res[0].(string)
	if !ok {
		a.fail(a.t, "unexpected type of return value")
		return "", nil, nil
	}

	if res[1] != nil {
		args, ok := res[1].([]interface{})
		if !ok {
			a.fail(a.t, "unexpected type of return value")
			return "", nil, nil
		}
		return statement, args, nil
	}

	return statement, nil, nil
}

func (a *Add) To(collection string) client.Add {
	a.t.Helper()
	res := a.Call(a.t, collection)
	if len(res) != 1 {
		a.fail(a.t, "unexpected length of return values")
		return nil
	}

	add, ok := res[0].(client.Add)
	if !ok {
		a.fail(a.t, "unexpected type of return value")
		return nil
	}

	return add
}

func (a *Add) Item(value map[string]interface{}) client.Add {
	a.t.Helper()
	res := a.Call(a.t, value)
	if len(res) != 1 {
		a.fail(a.t, "unexpected length of return values")
		return nil
	}

	add, ok := res[0].(client.Add)
	if !ok {
		a.fail(a.t, "unexpected type of return value")
		return nil
	}

	return add
}

// NewAdd creates a mock datastore.Add
func NewAdd(t *testing.T) *Add {
	a := Add{
		t: t,
	}

	if t != nil {
		a.fail = t.Fatal
	}

	return &a
}

// NewAddWithFail creates a mock datastore.Add with an expected failure
func NewAddWithFail(t *testing.T, expect ...interface{}) *Add {
	a := NewAdd(t)
	a.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return a
}
