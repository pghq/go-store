package mock

import (
	"context"
	"testing"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Update = NewUpdate(nil)
)

func (c *Client) Update() client.Update {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	update, ok := res[0].(client.Update)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return nil
	}

	return update
}

// Update is a mock datastore.Update
type Update struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (u *Update) Statement() (string, []interface{}, error) {
	u.t.Helper()
	res := u.Call(u.t)
	if len(res) != 3 {
		u.fail(u.t, "unexpected length of return values")
		return "", nil, nil
	}

	if res[2] != nil {
		err, ok := res[2].(error)
		if !ok {
			u.fail(u.t, "unexpected type of return value")
			return "", nil, nil
		}
		return "", nil, err
	}

	statement, ok := res[0].(string)
	if !ok {
		u.fail(u.t, "unexpected type of return value")
		return "", nil, nil
	}

	if res[1] != nil {
		args, ok := res[1].([]interface{})
		if !ok {
			u.fail(u.t, "unexpected type of return value")
			return "", nil, nil
		}
		return statement, args, nil
	}

	return statement, nil, nil
}

func (u *Update) In(collection string) client.Update {
	u.t.Helper()
	res := u.Call(u.t, collection)
	if len(res) != 1 {
		u.fail(u.t, "unexpected length of return values")
		return nil
	}

	update, ok := res[0].(client.Update)
	if !ok {
		u.fail(u.t, "unexpected type of return value")
		return nil
	}

	return update
}

func (u *Update) Item(snapshot map[string]interface{}) client.Update {
	u.t.Helper()
	res := u.Call(u.t, snapshot)
	if len(res) != 1 {
		u.fail(u.t, "unexpected length of return values")
		return nil
	}

	update, ok := res[0].(client.Update)
	if !ok {
		u.fail(u.t, "unexpected type of return value")
		return nil
	}

	return update
}

func (u *Update) Filter(filter interface{}) client.Update {
	u.t.Helper()
	res := u.Call(u.t, filter)
	if len(res) != 1 {
		u.fail(u.t, "unexpected length of return values")
		return nil
	}

	update, ok := res[0].(client.Update)
	if !ok {
		u.fail(u.t, "unexpected type of return value")
		return nil
	}

	return update
}

func (u *Update) Execute(ctx context.Context) (int, error) {
	u.t.Helper()
	res := u.Call(u.t, ctx)
	if len(res) != 2 {
		u.fail(u.t, "unexpected length of return values")
		return 0, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			u.fail(u.t, "unexpected type of return value")
			return 0, nil
		}
		return 0, err
	}

	count, ok := res[0].(int)
	if !ok {
		u.fail(u.t, "unexpected type of return value")
		return 0, nil
	}

	return count, nil
}

// NewUpdate creates a mock datastore.Update
func NewUpdate(t *testing.T) *Update {
	u := Update{
		t: t,
	}

	if t != nil {
		u.fail = t.Fatal
	}

	return &u
}
