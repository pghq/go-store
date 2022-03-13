package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"time"
)

const (
	// DefaultLimit Default limit for paginated queries
	DefaultLimit = 50

	// DefaultBatchReadSize Default batch read size
	DefaultBatchReadSize = 1

	// DefaultViewTTL Default view TTL
	DefaultViewTTL = 500 * time.Millisecond
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// DB A database technology
type DB interface {
	Txn(ctx context.Context, opts ...TxnOption) Txn
	Ping(ctx context.Context) error
}

// Txn A unit of work performed within a database
type Txn interface {
	Get(table string, k, v interface{}, args ...interface{}) error
	Insert(table string, k, v interface{}, args ...interface{}) error
	Update(table string, k, v interface{}, args ...interface{}) error
	Remove(table string, k interface{}, args ...interface{}) error
	List(table string, v interface{}, args ...interface{}) error
	Commit() error
	Rollback() error
}

// Config Database configuration
type Config struct {
	Schema             map[string]map[string][]string
	SQLOpenFunc        func(driverName, dataSourceName string) (*sql.DB, error)
	MigrationFS        fs.FS
	MigrationDirectory string
	MigrationTable     string
	PlaceholderPrefix  string
}

// ConfigWith Configure the database with custom ops
func ConfigWith(opts []Option) Config {
	config := Config{SQLOpenFunc: sql.Open}

	for _, opt := range opts {
		opt(&config)
	}

	return config
}

// Option A database option
type Option func(*Config)

// Storage defines the logical schema for the in-memory database
func Storage(o Schema) Option {
	return func(config *Config) {
		config.Schema = o
	}
}

// SQLOpen Custom SQL open func
func SQLOpen(o func(driverName, dataSourceName string) (*sql.DB, error)) Option {
	return func(config *Config) {
		config.SQLOpenFunc = o
	}
}

// Migrate Configure a database migration
func Migrate(fs fs.FS, directory, table string) Option {
	return func(config *Config) {
		config.MigrationFS = fs
		config.MigrationDirectory = directory
		config.MigrationTable = table
	}
}

// TxnConfig Transaction level configuration
type TxnConfig struct {
	ReadOnly      bool
	BatchWrite    bool
	ViewTTL       time.Duration
	BatchReadSize int
}

// TxnConfigWith Configure transaction with custom ops
func TxnConfigWith(opts []TxnOption) TxnConfig {
	config := TxnConfig{
		BatchReadSize: DefaultBatchReadSize,
		ViewTTL:       DefaultViewTTL,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return config
}

// TxnOption A transaction option
type TxnOption func(config *TxnConfig)

// ReadOnly Read only transactions
func ReadOnly() TxnOption {
	return func(config *TxnConfig) {
		config.ReadOnly = true
		config.BatchWrite = false
	}
}

// BatchWrite Batch write transactions
func BatchWrite() TxnOption {
	return func(config *TxnConfig) {
		config.BatchWrite = true
		config.ReadOnly = false
	}
}

// ViewTTL Cache time for transaction views
func ViewTTL(o time.Duration) TxnOption {
	return func(config *TxnConfig) {
		config.ViewTTL = o
	}
}

// BatchReadSize Batch read size for client side transactions (e.g., Redis Pipelines)
func BatchReadSize(o int) TxnOption {
	return func(config *TxnConfig) {
		config.BatchReadSize = o
	}
}

// Expire time to live for inserts
func Expire(o time.Duration) RequestOption {
	return func(req *Request) {
		req.TTL = o
		req.Expire = true
	}
}

// Request Database req
type Request struct {
	Page     int
	Limit    int
	OrderBy  []string
	GroupBy  []string
	Eq       []map[string]interface{}
	Px       map[string]string
	NotEq    []map[string]interface{}
	Lt       []map[string]interface{}
	Gt       []map[string]interface{}
	XEq      []map[string]interface{}
	NotXEq   []map[string]interface{}
	Tables   []Expression
	Filters  []Expression
	Suffix   []Expression
	Fields   map[string]Expression
	Expire   bool
	TTL      time.Duration
	CacheKey []interface{}
}

// HasFilter checks if the req has any filter params
func (q Request) HasFilter() bool {
	return q.Eq != nil || q.Px != nil || q.NotEq != nil || q.Lt != nil || q.Gt != nil || q.XEq != nil || q.NotXEq != nil
}

// NewRequest new database request
func NewRequest(args ...interface{}) *Request {
	req := Request{
		Limit:  DefaultLimit,
		Fields: make(map[string]Expression),
	}

	for _, arg := range args {
		if opts, ok := arg.([]RequestOption); ok {
			for _, opt := range opts {
				opt(&req)
			}
		}

		if opt, ok := arg.(RequestOption); ok {
			opt(&req)
		}
	}

	return &req
}

// RequestOption A req option
type RequestOption func(req *Request)

// Page Set a page offset
func Page(o int) RequestOption {
	return func(req *Request) {
		req.Page = o
		req.CacheKey = append(req.CacheKey, "page", o)
	}
}

// Limit Set a result limit
func Limit(o int) RequestOption {
	return func(req *Request) {
		req.Limit = o
		req.CacheKey = append(req.CacheKey, "limit", o)
	}
}

// OrderBy Order results by a field
func OrderBy(o string) RequestOption {
	return func(req *Request) {
		req.OrderBy = append(req.OrderBy, o)
		req.CacheKey = append(req.CacheKey, "orderBy", o)
	}
}

// GroupBy Group results
func GroupBy(o string) RequestOption {
	return func(req *Request) {
		req.GroupBy = append(req.GroupBy, o)
		req.CacheKey = append(req.CacheKey, "groupBy", o)
	}
}

// Eq Filter values where field equals value
func Eq(key string, values ...interface{}) RequestOption {
	return func(req *Request) {
		var v interface{} = values
		if len(values) == 1 {
			v = values[0]
		}

		req.Eq = append(req.Eq, map[string]interface{}{key: v})
		req.CacheKey = append(req.CacheKey, "eq", fmt.Sprintf("%s%+v", key, v))
	}
}

// NotEq Filter values where field does not equal value
func NotEq(key string, value interface{}) RequestOption {
	return func(req *Request) {
		req.NotEq = append(req.NotEq, map[string]interface{}{key: value})
		req.CacheKey = append(req.CacheKey, "neq", fmt.Sprintf("%s%+v", key, value))
	}
}

// Px Filter values where field matches prefix.
func Px(key, value string) RequestOption {
	return func(req *Request) {
		if req.Px == nil {
			req.Px = make(map[string]string)
		}
		req.Px[key] = value
		req.CacheKey = append(req.CacheKey, "px", fmt.Sprintf("%s%+v", key, value))
	}
}

// Lt Filter values where field is less than value
func Lt(key string, value interface{}) RequestOption {
	return func(req *Request) {
		req.Lt = append(req.Lt, map[string]interface{}{key: value})
		req.CacheKey = append(req.CacheKey, "lt", key, value)
	}
}

// Gt Filter values where field is greater than value
func Gt(key string, value interface{}) RequestOption {
	return func(req *Request) {
		req.Gt = append(req.Gt, map[string]interface{}{key: value})
		req.CacheKey = append(req.CacheKey, "gt", key, value)
	}
}

// XEq Filter values where field matches a regular expression
func XEq(key string, value interface{}) RequestOption {
	return func(req *Request) {
		req.XEq = append(req.XEq, map[string]interface{}{key: value})
		req.CacheKey = append(req.CacheKey, "xeq", fmt.Sprintf("%s%+v", key, value))
	}
}

// NotXEq Filter values where field does not match a regular expression
func NotXEq(key string, value interface{}) RequestOption {
	return func(req *Request) {
		req.NotXEq = append(req.NotEq, map[string]interface{}{key: value})
		req.CacheKey = append(req.CacheKey, "nxeq", fmt.Sprintf("%s%+v", key, value))
	}
}

// Filter raw filter
func Filter(filter string, args ...interface{}) RequestOption {
	return func(req *Request) {
		req.Filters = append(req.Filters, Expression{Format: filter, Args: args})
		req.CacheKey = append(req.CacheKey, "filter", fmt.Sprintf("%s%+v", filter, args))
	}
}

// Suffix for queries
func Suffix(suffix string, args ...interface{}) RequestOption {
	return func(req *Request) {
		req.Suffix = append(req.Suffix, Expression{Format: suffix, Args: args})
		req.CacheKey = append(req.CacheKey, "suffix", fmt.Sprintf("%s%+v", suffix, args))
	}
}

// Table raw filter
func Table(table string, args ...interface{}) RequestOption {
	return func(req *Request) {
		req.Tables = append(req.Tables, Expression{Format: table, Args: args})
		req.CacheKey = append(req.CacheKey, "table", fmt.Sprintf("%s%+v", table, args))
	}
}

// As specifies a field alias
func As(key, value string, args ...interface{}) RequestOption {
	return func(req *Request) {
		if _, present := req.Fields[key]; present {
			req.Fields[key] = Expression{Format: value, Args: args}
			req.CacheKey = append(req.CacheKey, "alias", key, value)
		}
	}
}

// Field gets the fields to return
func Field(field interface{}) RequestOption {
	return func(req *Request) {
		var fields []string
		for field, _ := range req.Fields {
			fields = append(fields, field)
		}

		switch v := field.(type) {
		case []string:
			if len(v) > 0 {
				fields = v
			}
		case string:
			fields = append(fields, v)
		default:
			if m, err := Map(v, true); err == nil {
				for field, _ := range m {
					fields = append(fields, field)
				}
			}
		}

		newFields := make(map[string]Expression)
		for _, field := range fields {
			field := ToSnakeCase(field)
			newFields[field] = Expression{Format: field}
		}

		req.Fields = newFields
		req.CacheKey = append(req.CacheKey, "fields", fields)
	}
}

// Expression Printf like formatted expression
type Expression struct {
	Format string
	Args   []interface{}
}

// ToSnakeCase converts a string to snake_case
// https://gist.github.com/stoewer/fbe273b711e6a06315d19552dd4d33e6
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
