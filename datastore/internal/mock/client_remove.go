package mock

import (
	"context"
	"testing"
	"time"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Remove = NewRemove(nil)
)

func (c *Client) Remove() client.Remove {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return nil
	}

	return remove
}

// Remove is a mock datastore.Remove
type Remove struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (r *Remove) Statement() (string, []interface{}, error) {
	r.t.Helper()
	res := r.Call(r.t)
	if len(res) != 3 {
		r.fail(r.t, "unexpected length of return values")
		return "", nil, nil
	}

	if res[2] != nil {
		err, ok := res[2].(error)
		if !ok {
			r.fail(r.t, "unexpected type of return value")
			return "", nil, nil
		}
		return "", nil, err
	}

	statement, ok := res[0].(string)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return "", nil, nil
	}

	if res[1] != nil {
		args, ok := res[1].([]interface{})
		if !ok {
			r.fail(r.t, "unexpected type of return value")
			return "", nil, nil
		}
		return statement, args, nil
	}

	return statement, nil, nil
}

func (r *Remove) Filter(filter client.Filter) client.Remove {
	r.t.Helper()
	res := r.Call(r.t, filter)
	if len(res) != 1 {
		r.fail(r.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return nil
	}

	return remove
}

func (r *Remove) Order(by string) client.Remove {
	r.t.Helper()
	res := r.Call(r.t, by)
	if len(res) != 1 {
		r.fail(r.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return nil
	}

	return remove
}

func (r *Remove) First(first int) client.Remove {
	r.t.Helper()
	res := r.Call(r.t, first)
	if len(res) != 1 {
		r.fail(r.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return nil
	}

	return remove
}

func (r *Remove) After(key string, value time.Time) client.Remove {
	r.t.Helper()
	res := r.Call(r.t, key, value)
	if len(res) != 1 {
		r.fail(r.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return nil
	}

	return remove
}

func (r *Remove) Execute(ctx context.Context) (int, error) {
	r.t.Helper()
	res := r.Call(r.t, ctx)
	if len(res) != 2 {
		r.fail(r.t, "unexpected length of return values")
		return 0, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			r.fail(r.t, "unexpected type of return value")
			return 0, nil
		}
		return 0, err
	}

	count, ok := res[0].(int)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return 0, nil
	}

	return count, nil
}

func (r *Remove) From(collection string) client.Remove {
	r.t.Helper()
	res := r.Call(r.t, collection)
	if len(res) != 1 {
		r.fail(r.t, "unexpected length of return values")
		return nil
	}

	remove, ok := res[0].(client.Remove)
	if !ok {
		r.fail(r.t, "unexpected type of return value")
		return nil
	}

	return remove
}

// NewRemove creates a mock datastore.Remove
func NewRemove(t *testing.T) *Remove {
	r := Remove{
		t: t,
	}

	if t != nil {
		r.fail = t.Fatal
	}

	return &r
}
