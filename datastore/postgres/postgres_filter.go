package postgres

import (
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Cond is an instance of filter conditions for pg queries
type Cond struct {
	err  error
	opts []squirrel.Sqlizer
}

// Eq is the = operator
func (f Cond) Eq(key string, value interface{}) Cond {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Eq slice")
		return f
	}

	return Cond{opts: append(f.opts, squirrel.Eq{key: value}), err: f.err}
}

// Lt is the < operator
func (f Cond) Lt(key string, value interface{}) Cond {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Lt slice")
		return f
	}

	return Cond{opts: append(f.opts, squirrel.Lt{key: value}), err: f.err}
}

// Gt is the > operator
func (f Cond) Gt(key string, value interface{}) Cond {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not Gt slice")
		return f
	}

	return Cond{opts: append(f.opts, squirrel.Gt{key: value}), err: f.err}
}

// NotEq is the <> operator
func (f Cond) NotEq(key string, value interface{}) Cond {
	if _, ok := value.([]interface{}); ok {
		f.err = errors.NewBadRequest("can not NotEq slice")
		return f
	}

	return Cond{opts: append(f.opts, squirrel.NotEq{key: value}), err: f.err}
}

// BeginsWith is the LIKE 'foo%' operation
func (f Cond) BeginsWith(key string, value string) Cond {
	return Cond{opts: append(f.opts, squirrel.ILike{key: fmt.Sprintf("%s%%", value)}), err: f.err}
}

// EndsWith is the LIKE '%foo' operation
func (f Cond) EndsWith(key string, value string) Cond {
	return Cond{opts: append(f.opts, squirrel.ILike{key: fmt.Sprintf("%%%s", value)}), err: f.err}
}

// Contains is the LIKE '%foo%' operation for strings or IN operator for arrays
func (f Cond) Contains(key string, value interface{}) Cond {
	if _, ok := value.(string); ok {
		return Cond{opts: append(f.opts, squirrel.ILike{key: fmt.Sprintf("%%%s%%", value)}), err: f.err}
	}

	if _, ok := value.([]interface{}); ok {
		return Cond{opts: append(f.opts, squirrel.Eq{key: value}), err: f.err}
	}

	return Cond{opts: append(f.opts, squirrel.Eq{key: []interface{}{value}}), err: f.err}
}

// NotContains is the NOT LIKE '%foo%' operation for strings or NOT IN operator for arrays
func (f Cond) NotContains(key string, value interface{}) Cond {
	if _, ok := value.(string); ok {
		return Cond{opts: append(f.opts, squirrel.NotILike{key: fmt.Sprintf("%%%s%%", value)}), err: f.err}
	}

	if _, ok := value.([]interface{}); ok {
		return Cond{opts: append(f.opts, squirrel.NotEq{key: value}), err: f.err}
	}

	return Cond{opts: append(f.opts, squirrel.NotEq{key: []interface{}{value}}), err: f.err}
}

// Or conjunction
func (f Cond) Or(another Cond) Cond {
	return Cond{opts: append(f.opts, squirrel.Or{f, another}), err: f.err}
}

// And conjunction
func (f Cond) And(another Cond) Cond {
	return Cond{opts: append(f.opts, squirrel.And{f, another}), err: f.err}
}

// Raw constructs a raw pg Cond
func (f Cond) Raw(sql string, args ...interface{}) Cond {
	return Cond{opts: append(f.opts, squirrel.Expr(sql, args...)), err: f.err}
}

func (f Cond) ToSql() (string, []interface{}, error) {
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
