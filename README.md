# go-ark
Go data mapper providing a familiar API for internally supported database providers.

## Installation

go-ark may be installed using the go get command:

```
go get github.com/pghq/go-ark
```
## Usage

A typical usage scenario:

```
import (
    "context"
    
    "github.com/pghq/go-ark/database"
    "github.com/pghq/go-ark"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Open a postgres connection
db, err := ark.New("postgres://user:pass@postgres/db", database.Migrate(migrations))
if err != nil{
    panic(err)
}

// Create a transaction
tx := db.Txn(context.Background())
defer tx.Rollback()

// Commit some data
if err := tx.Insert("foos", map[string]interface{}{"id": "foo"}); err != nil{
    panic(err)
}

if err := tx.Commit(); err != nil{
    panic(err)
}
```

## Supported Providers
- Postgres
- Redshift