package db

import (
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	opts := []Option{
		RDB(Schema{}),
		DSN(""),
		SQL(nil),
		SQLTrace(nil),
		SQLOpen(nil),
		DriverName(""),
		MaxConns(0),
		MaxIdleLifetime(0),
		MaxConnLifetime(0),
		Migration(nil, "", ""),
		Redis(redis.Options{}),
	}
	config := ConfigWith(opts)
	assert.NotEqual(t, Config{}, config)
}

func TestTxnOption(t *testing.T) {
	opts := []TxnOption{
		ReadOnly(),
		BatchWrite(),
		ViewTTL(0),
		BatchReadSize(0),
	}
	config := TxnConfigWith(opts)
	assert.NotEqual(t, TxnConfig{}, config)
}

func TestCommandOption(t *testing.T) {
	opts := []CommandOption{
		TTL(0),
		CommandKey(""),
		CommandSQLPlaceholder(""),
	}
	cmd := CommandWith(opts)
	assert.NotEqual(t, Command{}, cmd)
}

func TestQueryOption(t *testing.T) {
	opts := []QueryOption{
		QueryKey(""),
		QuerySQLPlaceholder(""),
		Eq("", "bar4"),
		NotEq("", ""),
		Fields("", ""),
		XEq("", ""),
		NotXEq("", ""),
		Page(0),
		Limit(0),
		OrderBy(""),
		Gt("", 0),
		Lt("", 0),
		Expr(""),
	}
	query := QueryWith(opts)
	assert.NotEqual(t, Query{}, query)
}
