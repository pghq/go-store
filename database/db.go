package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
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
	Get(ctx context.Context, table string, q Query, v interface{}) error
	Insert(ctx context.Context, table string, v interface{}) error
	Update(ctx context.Context, table string, q Query, v interface{}) error
	Remove(ctx context.Context, table string, q Query) error
	List(ctx context.Context, table string, q Query, v interface{}) error
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

// Query Database q
type Query struct {
	Page             int
	Limit            int
	OrderBy          []string
	GroupBy          []string
	Eq               map[string]interface{}
	Px               map[string]string
	NotEq            map[string]interface{}
	Lt               map[string]interface{}
	Gt               map[string]interface{}
	XEq              map[string]interface{}
	NotXEq           map[string]interface{}
	Alias            map[string]string
	Options          []string
	Tables           []Expression
	Filters          []Expression
	Suffixes         []Expression
	Fields           []string
	AdditionalFields []string
	Table            string
	Format           squirrel.PlaceholderFormat
}

type Expression interface {
	ToSql() (string, []interface{}, error)
}

func (q Query) Key(table string) []byte {
	return []byte(fmt.Sprintf("%s.%+v", table, map[string]interface{}{
		"page":     q.Page,
		"limit":    q.Limit,
		"orderBy":  q.OrderBy,
		"groupBy":  q.GroupBy,
		"eq":       q.Eq,
		"px":       q.Px,
		"neq":      q.NotEq,
		"lt":       q.Lt,
		"gt":       q.Gt,
		"xeq":      q.XEq,
		"nxeq":     q.NotXEq,
		"tables":   q.Tables,
		"filters":  q.Filters,
		"suffixes": q.Suffixes,
		"fields":   append(q.Fields, q.AdditionalFields...),
		"options":  q.Options,
		"table":    q.Table,
		"format":   q.Format,
	}))
}

func (q Query) ToSql() (string, []interface{}, error) {
	format := squirrel.PlaceholderFormat(squirrel.Question)
	if q.Format != nil {
		format = q.Format
	}

	builder := squirrel.StatementBuilder.
		Select().
		Options(q.Options...).
		From(q.Table).
		OrderBy(q.OrderBy...).
		GroupBy(q.GroupBy...).
		Where(squirrel.Eq(q.Eq)).
		Where(squirrel.NotEq(q.NotEq)).
		Where(squirrel.Lt(q.Lt)).
		Where(squirrel.Gt(q.Gt)).
		Where(squirrel.Like(q.XEq)).
		Where(squirrel.NotLike(q.NotXEq)).
		PlaceholderFormat(format)

	if q.Limit > 0 {
		builder = builder.Limit(uint64(q.Limit))
	}

	for _, field := range q.Fields {
		column := interface{}(field)
		if expr, present := q.Alias[field]; present {
			column = squirrel.Alias(squirrel.Expr(expr), field)
		}
		builder = builder.Column(column)
	}

	for _, field := range q.AdditionalFields {
		column := interface{}(field)
		builder = builder.Column(column)
	}

	for _, expr := range q.Tables {
		builder = builder.JoinClause(expr)
	}

	for k, v := range q.Px {
		builder = builder.Where(squirrel.ILike(map[string]interface{}{k: fmt.Sprintf("%s%%", v)}))
	}

	for _, expr := range q.Filters {
		builder = builder.Where(expr)
	}

	for _, expr := range q.Suffixes {
		builder = builder.SuffixExpr(expr)
	}

	return builder.ToSql()
}

// Expr shorthand
func Expr(format string, args ...interface{}) Expression {
	return expression{Format: format, Args: args}
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

// expression Printf like formatted expression
type expression struct {
	Format string
	Args   []interface{}
}

func (e expression) ToSql() (string, []interface{}, error) {
	return squirrel.Expr(e.Format, e.Args...).ToSql()
}

// ToSnakeCase converts a string to snake_case
// https://gist.github.com/stoewer/fbe273b711e6a06315d19552dd4d33e6
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
