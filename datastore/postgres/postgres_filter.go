package postgres

import (
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

func (c *Client) Filter() client.Filter {
	return Filter()
}

type filter struct {
	err  error
	opts []squirrel.Sqlizer
}

func (f filter) Eq(key string, value interface{}) client.Filter {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Eq slice")
		return f
	}

	return filter{opts: append(f.opts, squirrel.Eq{key: value}), err: f.err}
}

func (f filter) Lt(key string, value interface{}) client.Filter {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Lt slice")
		return f
	}

	return filter{opts: append(f.opts, squirrel.Lt{key: value}), err: f.err}
}

func (f filter) Gt(key string, value interface{}) client.Filter {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Gt slice")
		return f
	}

	return filter{opts: append(f.opts, squirrel.Gt{key: value}), err: f.err}
}

func (f filter) IsNil() bool {
	return len(f.opts) == 0
}

func (f filter) NotEq(key string, value interface{}) client.Filter {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not NotEq slice")
		return f
	}

	return filter{opts: append(f.opts, squirrel.NotEq{key: value}), err: f.err}
}

func (f filter) BeginsWith(key string, value string) client.Filter {
	return filter{opts: append(f.opts, squirrel.Like{key: fmt.Sprintf("%%%s", value)}), err: f.err}
}

func (f filter) EndsWith(key string, value string) client.Filter {
	return filter{opts: append(f.opts, squirrel.Like{key: fmt.Sprintf("%s%%", value)}), err: f.err}
}

func (f filter) Contains(key string, value interface{}) client.Filter {
	if _, ok := value.(string); ok {
		return filter{opts: append(f.opts, squirrel.Like{key: fmt.Sprintf("%%%s%%", value)}), err: f.err}
	}

	if _, ok := value.([]interface{}); ok {
		return filter{opts: append(f.opts, squirrel.Eq{key: value}), err: f.err}
	}

	return filter{opts: append(f.opts, squirrel.Eq{key: []interface{}{value}}), err: f.err}
}

func (f filter) NotContains(key string, value interface{}) client.Filter {
	if _, ok := value.(string); ok {
		return filter{opts: append(f.opts, squirrel.NotLike{key: fmt.Sprintf("%%%s%%", value)}), err: f.err}
	}

	if _, ok := value.([]interface{}); ok {
		return filter{opts: append(f.opts, squirrel.NotEq{key: value}), err: f.err}
	}

	return filter{opts: append(f.opts, squirrel.NotEq{key: []interface{}{value}}), err: f.err}
}

func (f filter) Or(another client.Filter) client.Filter {
	if or, ok := another.(filter); ok {
		return filter{opts: append(f.opts, squirrel.Or{f, or}), err: f.err}
	}

	f.err = errors.NewBadRequest("can not Or value of unknown type")
	return f
}

func (f filter) And(another client.Filter) client.Filter {
	if and, ok := another.(filter); ok {
		return filter{opts: append(f.opts, squirrel.And{f, and}), err: f.err}
	}

	f.err = errors.NewBadRequest("can not And value of unknown type")

	return f
}

func (f filter) ToSql() (string, []interface{}, error) {
	if f.err != nil {
		return "", nil, f.err
	}

	var statements []string
	var arguments []interface{}

	for _, opt := range f.opts {
		sql, args, err := opt.ToSql()
		if err != nil {
			return "", nil, errors.BadRequest(err)
		}
		statements = append(statements, sql)
		arguments = append(arguments, args...)
	}

	return strings.Join(statements, " AND "), arguments, nil
}

func Filter() client.Filter {
	return filter{}
}
