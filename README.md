# go-store
High-performance persistence library for internal Go apps.

## Installation

go-store may be installed using the go get command:

```
go get github.com/pghq/go-store
```
## Usage

A typical usage scenario:

```
import (
    "context"
    
    "github.com/pghq/go-store/database"
    "github.com/pghq/go-store"
)

//go:embed schema
var schema embed.FS

// Open a postgres connection
db, err := ark.New("postgres://user:pass@postgres/db", database.Migrate(schema))
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