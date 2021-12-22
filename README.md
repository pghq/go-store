# go-ark
Go data mapper for internally supported providers.

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
    
    "github.com/pghq/go-ark"
)

// Open an in-memory data mapper
db := ark.New("memory://")

// Create a transaction
tx := db.Txn(context.Background())
defer tx.Rollback()

// Commit some data
err := tx.Insert("", []byte("dog"), "roof")
if err != nil{
    panic(err)
}

err := tx.InsertTTL("", []byte("cat"), "meow", 0)
if err != nil{
    panic(err)
}

if err := tx.Commit(); err != nil{
    panic(err)
}
```

## Supported Providers
- In-Memory
- Redis
- SQL