package mock

import (
	"testing"

	"github.com/pghq/go-ark/client"
)

var (
	_ client.Client = NewClient(nil)
)

// Client is a mock datastore.Client
type Client struct {
	client.Client
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (c *Client) Connect() error {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			c.fail(c.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

// NewDisconnectedClient creates a new disconnected mock store
func NewDisconnectedClient(t *testing.T) *Client {
	c := Client{
		t: t,
	}

	if t != nil {
		c.fail = t.Fatal
	}

	return &c
}

// NewClient creates a connected mock store
func NewClient(t *testing.T) *Client {
	c := NewDisconnectedClient(t)
	c.Expect("Connect").Return(nil)

	return c
}
