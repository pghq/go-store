package mock

import (
	"testing"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Filter = NewFilter(nil)
)

func (c *Client) Filter() client.Filter {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return nil
	}

	return filter
}

// Filter is a mock datastore.Filter
type Filter struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (f *Filter) BeginsWith(key string, prefix string) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, prefix)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) EndsWith(key string, suffix string) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, suffix)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Contains(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) NotContains(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Eq(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Lt(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Gt(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) NotEq(key string, value interface{}) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) IsNil() bool {
	return f == nil
}

func (f *Filter) Or(another client.Filter) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, another)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) And(another client.Filter) client.Filter {
	f.t.Helper()
	res := f.Call(f.t, another)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(client.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

// NewFilter creates a mock datastore.Filter
func NewFilter(t *testing.T) *Filter {
	f := Filter{
		t: t,
	}

	if t != nil {
		f.fail = t.Fatal
	}

	return &f
}
