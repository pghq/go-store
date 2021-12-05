package internal

import (
	"bytes"
	"fmt"
	"strings"
)

// Stmt represents a sql statement encoder
type Stmt interface {
	Bytes() []byte
	SQL(pp SQLPlaceholderPrefix) (string, []interface{}, error)
	StandardMethod() StandardMethod
}

// StandardMethod is an instance of a CRUDL provider method
type StandardMethod struct {
	Table  string
	Key    []byte
	Value  interface{}
	Filter interface{}
	Insert bool
	Update bool
	List   bool
	Get    bool
	Remove bool
}

// SQLPlaceholderPrefix is a custom placeholder prefix for SQL
type SQLPlaceholderPrefix string

func (s SQLPlaceholderPrefix) ReplacePlaceholders(sql string) (string, error){
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" {
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			sql = sql[p+2:]
		} else {
			i++
			buf.WriteString(sql[:p])
			_, _ = fmt.Fprintf(buf, "%s%d", s, i)
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String(), nil
}
