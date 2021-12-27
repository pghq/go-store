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
	Get(table string, k Key, v interface{}, opts ...QueryOption) error
	Insert(table string, k Key, v interface{}, opts ...CommandOption) error
	Update(table string, k Key, v interface{}, opts ...CommandOption) error
	Remove(table string, k Key, opts ...CommandOption) error
	List(table string, v interface{}, opts ...QueryOption) error
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

// Command A database command
type Command struct {
	Expire bool
	TTL    time.Duration
}

// CommandWith Configure command with custom ops
func CommandWith(opts []CommandOption) Command {
	cmd := Command{}
	for _, opt := range opts {
		opt(&cmd)
	}

	return cmd
}

// CommandOption A command option
type CommandOption func(*Command)

// Expire time to live for inserts
func Expire(o time.Duration) CommandOption {
	return func(cmd *Command) {
		cmd.TTL = o
		cmd.Expire = true
	}
}

// Query Database query
type Query struct {
	Page     int
	Limit    int
	OrderBy  []string
	GroupBy  []string
	Eq       []map[string]interface{}
	NotEq    []map[string]interface{}
	Lt       []map[string]interface{}
	Gt       []map[string]interface{}
	XEq      []map[string]interface{}
	NotXEq   []map[string]interface{}
	Tables   []Expression
	Filters  []Expression
	Fields   map[string]string
	CacheKey []interface{}
}

// HasFilter checks if the query has any filter params
func (q Query) HasFilter() bool {
	return q.Eq != nil || q.NotEq != nil || q.Lt != nil || q.Gt != nil || q.XEq != nil || q.NotXEq != nil
}

// QueryWith Configure query with custom ops
func QueryWith(opts []QueryOption) Query {
	query := Query{
		Limit:  DefaultLimit,
		Fields: make(map[string]string),
	}

	for _, opt := range opts {
		opt(&query)
	}

	return query
}

// QueryOption A query option
type QueryOption func(query *Query)

// Page Set a page offset
func Page(o int) QueryOption {
	return func(query *Query) {
		query.Page = o
		query.CacheKey = append(query.CacheKey, "page", o)
	}
}

// Limit Set a result limit
func Limit(o int) QueryOption {
	return func(query *Query) {
		query.Limit = o
		query.CacheKey = append(query.CacheKey, "limit", o)
	}
}

// OrderBy Order results by a field
func OrderBy(o string) QueryOption {
	return func(query *Query) {
		query.OrderBy = append(query.OrderBy, o)
		query.CacheKey = append(query.CacheKey, "orderBy", o)
	}
}

// GroupBy Group results
func GroupBy(o string) QueryOption {
	return func(query *Query) {
		query.GroupBy = append(query.GroupBy, o)
		query.CacheKey = append(query.CacheKey, "groupBy", o)
	}
}

// Eq Filter values where field equals value
func Eq(key string, values ...interface{}) QueryOption {
	return func(query *Query) {
		var v interface{} = values
		if len(values) == 1 {
			v = values[0]
		}

		query.Eq = append(query.Eq, map[string]interface{}{key: v})
		query.CacheKey = append(query.CacheKey, "eq", fmt.Sprintf("%s%+v", key, v))
	}
}

// NotEq Filter values where field does not equal value
func NotEq(key string, value interface{}) QueryOption {
	return func(query *Query) {
		query.NotEq = append(query.NotEq, map[string]interface{}{key: value})
		query.CacheKey = append(query.CacheKey, "neq", fmt.Sprintf("%s%+v", key, value))
	}
}

// Lt Filter values where field is less than value
func Lt(key string, value interface{}) QueryOption {
	return func(query *Query) {
		query.Lt = append(query.Lt, map[string]interface{}{key: value})
		query.CacheKey = append(query.CacheKey, "lt", key, value)
	}
}

// Gt Filter values where field is greater than value
func Gt(key string, value interface{}) QueryOption {
	return func(query *Query) {
		query.Gt = append(query.Gt, map[string]interface{}{key: value})
		query.CacheKey = append(query.CacheKey, "gt", key, value)
	}
}

// XEq Filter values where field matches a regular expression
func XEq(key string, value interface{}) QueryOption {
	return func(query *Query) {
		query.XEq = append(query.XEq, map[string]interface{}{key: value})
		query.CacheKey = append(query.CacheKey, "xeq", fmt.Sprintf("%s%+v", key, value))
	}
}

// NotXEq Filter values where field does not match a regular expression
func NotXEq(key string, value interface{}) QueryOption {
	return func(query *Query) {
		query.NotXEq = append(query.NotEq, map[string]interface{}{key: value})
		query.CacheKey = append(query.CacheKey, "nxeq", fmt.Sprintf("%s%+v", key, value))
	}
}

// Filter raw filter
func Filter(filter string, args ...interface{}) QueryOption {
	return func(query *Query) {
		query.Filters = append(query.Filters, Expression{Format: filter, Args: args})
		query.CacheKey = append(query.CacheKey, "filter", fmt.Sprintf("%s%+v", filter, args))
	}
}

// Table raw filter
func Table(table string, args ...interface{}) QueryOption {
	return func(query *Query) {
		query.Tables = append(query.Tables, Expression{Format: table, Args: args})
		query.CacheKey = append(query.CacheKey, "table", fmt.Sprintf("%s%+v", table, args))
	}
}

// As specifies a field alias
func As(key, value string) QueryOption {
	return func(query *Query) {
		if _, present := query.Fields[key]; present {
			query.Fields[key] = value
			query.CacheKey = append(query.CacheKey, "alias", key, value)
		}
	}
}

// Field gets the fields to return
func Field(field interface{}) QueryOption {
	return func(query *Query) {
		var fields []string
		for field, _ := range query.Fields {
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

		newFields := make(map[string]string)
		for _, field := range fields {
			field := ToSnakeCase(field)
			newFields[field] = field
		}

		query.Fields = newFields
		query.CacheKey = append(query.CacheKey, "fields", fields)
	}
}

// Expression Printf like formatted expression
type Expression struct {
	Format string
	Args   []interface{}
}

// Key is a database key
type Key struct {
	Name  string
	Value interface{}
}

func (k Key) String() string {
	return fmt.Sprintf("%s%s", k.Name, k.Value)
}

// NamedKey creates a new named db ky
func NamedKey(name, k interface{}) Key {
	return Key{
		Name:  KeyName(name),
		Value: k,
	}
}

// Id creates a new string key named id
func Id(k string) Key {
	return NamedKey("id", k)
}

// ToSnakeCase converts a string to snake_case
// https://gist.github.com/stoewer/fbe273b711e6a06315d19552dd4d33e6
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
