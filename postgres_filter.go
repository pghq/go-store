package ark

import (
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-tea"
)

// PgCond is an instance of filter conditions for pg queries
type PgCond struct {
	err  error
	opts []squirrel.Sqlizer
}

// Eq is the = operator
func (c PgCond) Eq(key string, value interface{}) PgCond {
	if _, ok := value.([]interface{}); ok {
		c.err = tea.NewBadRequest("can not Eq slice")
		return c
	}

	return PgCond{opts: append(c.opts, squirrel.Eq{key: value}), err: c.err}
}

// Lt is the < operator
func (c PgCond) Lt(key string, value interface{}) PgCond {
	if _, ok := value.([]interface{}); ok {
		c.err = tea.NewBadRequest("can not Lt slice")
		return c
	}

	return PgCond{opts: append(c.opts, squirrel.Lt{key: value}), err: c.err}
}

// Gt is the > operator
func (c PgCond) Gt(key string, value interface{}) PgCond {
	if _, ok := value.([]interface{}); ok {
		c.err = tea.NewBadRequest("can not Gt slice")
		return c
	}

	return PgCond{opts: append(c.opts, squirrel.Gt{key: value}), err: c.err}
}

// NotEq is the <> operator
func (c PgCond) NotEq(key string, value interface{}) PgCond {
	if _, ok := value.([]interface{}); ok {
		c.err = tea.NewBadRequest("can not NotEq slice")
		return c
	}

	return PgCond{opts: append(c.opts, squirrel.NotEq{key: value}), err: c.err}
}

// BeginsWith is the LIKE 'foo%' operation
func (c PgCond) BeginsWith(key string, value string) PgCond {
	return PgCond{opts: append(c.opts, squirrel.ILike{key: fmt.Sprintf("%s%%", value)}), err: c.err}
}

// EndsWith is the LIKE '%foo' operation
func (c PgCond) EndsWith(key string, value string) PgCond {
	return PgCond{opts: append(c.opts, squirrel.ILike{key: fmt.Sprintf("%%%s", value)}), err: c.err}
}

// Contains is the LIKE '%foo%' operation for strings or IN operator for arrays
func (c PgCond) Contains(key string, value interface{}) PgCond {
	if _, ok := value.(string); ok {
		return PgCond{opts: append(c.opts, squirrel.ILike{key: fmt.Sprintf("%%%s%%", value)}), err: c.err}
	}

	if _, ok := value.([]interface{}); ok {
		return PgCond{opts: append(c.opts, squirrel.Eq{key: value}), err: c.err}
	}

	return PgCond{opts: append(c.opts, squirrel.Eq{key: []interface{}{value}}), err: c.err}
}

// NotContains is the NOT LIKE '%foo%' operation for strings or NOT IN operator for arrays
func (c PgCond) NotContains(key string, value interface{}) PgCond {
	if _, ok := value.(string); ok {
		return PgCond{opts: append(c.opts, squirrel.NotILike{key: fmt.Sprintf("%%%s%%", value)}), err: c.err}
	}

	if _, ok := value.([]interface{}); ok {
		return PgCond{opts: append(c.opts, squirrel.NotEq{key: value}), err: c.err}
	}

	return PgCond{opts: append(c.opts, squirrel.NotEq{key: []interface{}{value}}), err: c.err}
}

// Or conjunction
func (c PgCond) Or(another PgCond) PgCond {
	return PgCond{opts: append(c.opts, squirrel.Or{c, another}), err: c.err}
}

// And conjunction
func (c PgCond) And(another PgCond) PgCond {
	return PgCond{opts: append(c.opts, squirrel.And{c, another}), err: c.err}
}

// Raw constructs a raw pg PgCond
func (c PgCond) Raw(sql string, args ...interface{}) PgCond {
	return PgCond{opts: append(c.opts, squirrel.Expr(sql, args...)), err: c.err}
}

func (c PgCond) ToSql() (string, []interface{}, error) {
	if c.err != nil {
		return "", nil, c.err
	}

	var statements []string
	var arguments []interface{}

	for _, opt := range c.opts {
		sql, args, err := opt.ToSql()
		if err != nil {
			return "", nil, tea.BadRequest(err)
		}
		statements = append(statements, sql)
		arguments = append(arguments, args...)
	}

	return strings.Join(statements, " AND "), arguments, nil
}
