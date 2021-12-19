package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/fs"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// DefaultLimit Default limit for paginated queries
	DefaultLimit = 50

	// DefaultMaxConns Default max connections
	DefaultMaxConns = 100

	// DefaultMaxIdleLifetime Default maximum idle time
	DefaultMaxIdleLifetime = 5 * time.Minute

	// DefaultBatchReadSize Default batch read size
	DefaultBatchReadSize = 1

	// DefaultViewTTL Default view TTL
	DefaultViewTTL = 500 * time.Millisecond
)

// DB A database technology
type DB interface {
	Txn(ctx context.Context, opts ...TxnOption) Txn
	Ping(ctx context.Context) error
}

// Txn A unit of work performed within a database
type Txn interface {
	Get(table string, k, v interface{}, opts ...QueryOption) error
	Insert(table string, k, v interface{}, opts ...CommandOption) error
	Update(table string, k, v interface{}, opts ...CommandOption) error
	Remove(table string, k interface{}, opts ...CommandOption) error
	List(table string, v interface{}, opts ...QueryOption) error
	Commit() error
	Rollback() error
}

// Schema Schema for in-memory database
type Schema map[string]map[string][]string

// Config Database configuration
type Config struct {
	Schema             map[string]map[string][]string
	DSN                string
	SQL                *sql.DB
	SQLOpenFunc        func(driverName, dataSourceName string) (*sql.DB, error)
	SQLTraceDriver     driver.Driver
	DriverName         string
	MaxConns           int
	MaxConnLifetime    time.Duration
	MaxIdleLifetime    time.Duration
	MigrationFS        fs.FS
	MigrationDirectory string
	MigrationTable     string
	PlaceholderPrefix  string
	RedisOptions       redis.Options
}

// ConfigWith Configure the database with custom ops
func ConfigWith(opts []Option) Config {
	config := Config{
		MaxConns:        DefaultMaxConns,
		MaxIdleLifetime: DefaultMaxIdleLifetime,
		SQLOpenFunc:     sql.Open,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return config
}

// Option A database option
type Option func(*Config)

// RDB In-memory database schema option
func RDB(o Schema) Option {
	return func(config *Config) {
		config.Schema = o
	}
}

// DSN Database DSN option
func DSN(o string) Option {
	return func(config *Config) {
		config.DSN = o
	}
}

// SQL Custom SQL database backend
func SQL(o *sql.DB) Option {
	return func(config *Config) {
		config.SQL = o
	}
}

// SQLTrace Enable logging db statements
func SQLTrace(o driver.Driver) Option {
	return func(config *Config) {
		config.SQLTraceDriver = o
	}
}

// SQLOpen Custom SQL open func
func SQLOpen(o func(driverName, dataSourceName string) (*sql.DB, error)) Option {
	return func(config *Config) {
		config.SQLOpenFunc = o
	}
}

// DriverName Configure database driver name
func DriverName(o string) Option {
	return func(config *Config) {
		config.DriverName = o
	}
}

// MaxConns Set maximum conns
func MaxConns(o int) Option {
	return func(config *Config) {
		config.MaxConns = o
	}
}

// MaxIdleLifetime Set max idle lifetime
func MaxIdleLifetime(o time.Duration) Option {
	return func(config *Config) {
		config.MaxIdleLifetime = o
	}
}

// MaxConnLifetime Set max conn lifetime
func MaxConnLifetime(o time.Duration) Option {
	return func(config *Config) {
		config.MaxConnLifetime = o
	}
}

// Migration Configure a database migration
func Migration(fs fs.FS, directory, table string) Option {
	return func(config *Config) {
		config.MigrationFS = fs
		config.MigrationDirectory = directory
		config.MigrationTable = table
	}
}

// Redis Configure redis
func Redis(o redis.Options) Option {
	return func(config *Config) {
		config.RedisOptions = o
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
	Expire         bool
	TTL            time.Duration
	KeyName        string
	SQLPlaceholder string
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

// TTL TTL for inserts
func TTL(o time.Duration) CommandOption {
	return func(cmd *Command) {
		cmd.TTL = o
		cmd.Expire = true
	}
}

// CommandKey Key name / column for primaries
func CommandKey(o string) CommandOption {
	return func(cmd *Command) {
		cmd.KeyName = o
	}
}

// CommandSQLPlaceholder Custom SQL placeholder prefix (e.g., "$")
func CommandSQLPlaceholder(o string) CommandOption {
	return func(cmd *Command) {
		cmd.SQLPlaceholder = o
	}
}

// Query Database query
type Query struct {
	Page           int
	Limit          int
	OrderBy        []string
	KeyName        string
	Eq             []map[string]interface{}
	NotEq          []map[string]interface{}
	Lt             []map[string]interface{}
	Gt             []map[string]interface{}
	XEq            []map[string]interface{}
	NotXEq         []map[string]interface{}
	Tables         []Expression
	Filters        []Expression
	Fields         []string
	CacheKey       []interface{}
	SQLPlaceholder string
}

// HasFilter checks if the query has any filter params
func (q Query) HasFilter() bool {
	return q.Eq != nil || q.NotEq != nil || q.Lt != nil || q.Gt != nil || q.XEq != nil || q.NotXEq != nil
}

// QueryWith Configure query with custom ops
func QueryWith(opts []QueryOption) Query {
	query := Query{
		Limit: DefaultLimit,
	}

	for _, opt := range opts {
		opt(&query)
	}

	return query
}

// QueryOption A query option
type QueryOption func(query *Query)

// QueryKey Key name / column for primaries
func QueryKey(o string) QueryOption {
	return func(query *Query) {
		query.KeyName = o
		query.CacheKey = append(query.CacheKey, "key", o)
	}
}

// QuerySQLPlaceholder Custom SQL placeholder prefix (e.g., "$")
func QuerySQLPlaceholder(o string) QueryOption {
	return func(query *Query) {
		query.SQLPlaceholder = o
	}
}

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
		query.CacheKey = append(query.CacheKey, "filter", fmt.Sprintf(filter, args...))
	}
}

// Table raw filter
func Table(table string, args ...interface{}) QueryOption {
	return func(query *Query) {
		query.Tables = append(query.Tables, Expression{Format: table, Args: args})
		query.CacheKey = append(query.CacheKey, "table", fmt.Sprintf(table, args...))
	}
}

// Fields gets the fields to return
func Fields(args ...interface{}) QueryOption {
	var fields []string
	for _, arg := range args {
		switch v := arg.(type) {
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
	}

	return func(query *Query) {
		query.Fields = append(query.Fields, fields...)
		query.CacheKey = append(query.CacheKey, "fields", fields)
	}
}

// Expression Printf like formatted expression
type Expression struct {
	Format string
	Args   []interface{}
}
