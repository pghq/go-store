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
	DefaultLimit = 25

	// DefaultViewTTL Default view TTL
	DefaultViewTTL = 100 * time.Millisecond
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// Driver A database technology
type Driver interface {
	Txn(ctx context.Context, opts ...TxnOption) Txn
	Ping(ctx context.Context) error
}

// Txn A unit of work performed within a database
type Txn interface {
	Get(ctx context.Context, table string, query Query, v interface{}) error
	Insert(ctx context.Context, table string, v interface{}) error
	Update(ctx context.Context, table string, query Query, v interface{}) error
	Remove(ctx context.Context, table string, query Query) error
	List(ctx context.Context, table string, query Query, v interface{}) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Config Database configuration
type Config struct {
	SQLOpenFunc        func(driverName, dataSourceName string) (*sql.DB, error)
	MigrationFS        fs.ReadDirFS
	MigrationDirectory string
	SeedDirectory      string
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

// SQLOpen Custom SQL open func
func SQLOpen(o func(driverName, dataSourceName string) (*sql.DB, error)) Option {
	return func(config *Config) {
		config.SQLOpenFunc = o
	}
}

// Migrate Configure a database migration
func Migrate(fs fs.ReadDirFS) Option {
	return func(config *Config) {
		config.MigrationFS = fs
		config.MigrationDirectory = "schema/migrations"
		config.SeedDirectory = "schema/seed"
		config.MigrationTable = "migrations"
	}
}

// MigrateDirectory Configure a database migration with custom table and directory
func MigrateDirectory(directory, table string) Option {
	return func(config *Config) {
		config.MigrationDirectory = directory
		config.MigrationTable = table
	}
}

// SeedDirectory Configure a database migration with custom seed directory
func SeedDirectory(directory string) Option {
	return func(config *Config) {
		config.SeedDirectory = directory
	}
}

// TxnConfig Transaction level configuration
type TxnConfig struct {
	ReadOnly   bool
	BatchWrite bool
	ViewTTL    time.Duration
}

// TxnConfigWith Configure transaction with custom ops
func TxnConfigWith(opts []TxnOption) TxnConfig {
	config := TxnConfig{ViewTTL: DefaultViewTTL}

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

// Query Database query
type Query struct {
	Page    int
	Limit   int
	OrderBy []string
	GroupBy []string
	Eq      map[string]interface{}
	Px      map[string]string
	NotEq   map[string]interface{}
	Lt      map[string]interface{}
	Gt      map[string]interface{}
	XEq     map[string]interface{}
	NotXEq  map[string]interface{}
	Alias   map[string]string
	Tables  []Expression
	Filters []Expression
	Fields  []string
}

func (q Query) Key(table string) []byte {
	return []byte(fmt.Sprintf("%s.%+v", table, map[string]interface{}{
		"page":    q.Page,
		"limit":   q.Limit,
		"orderBy": q.OrderBy,
		"groupBy": q.GroupBy,
		"eq":      q.Eq,
		"px":      q.Px,
		"neq":     q.NotEq,
		"lt":      q.Lt,
		"gt":      q.Gt,
		"xeq":     q.XEq,
		"nxeq":    q.NotXEq,
		"tables":  q.Tables,
		"filters": q.Filters,
		"fields":  q.Fields,
	}))
}

// Expr shorthand
func Expr(format string, args ...interface{}) Expression {
	return Expression{Format: format, Args: args}
}

// AppendFields gets the fields to return
func AppendFields(slice []string, elems ...interface{}) []string {
	var fields []string
	for _, field := range slice {
		fields = append(fields, field)
	}

	for _, field := range elems {
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
	}

	newFields := make([]string, len(fields))
	for i, field := range fields {
		field := ToSnakeCase(field)
		newFields[i] = field
	}

	return newFields
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
