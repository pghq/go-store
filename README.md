# go-ark
Go data mappers for internally supported database providers.

## Installation

go-ark may be installed using the go get command:

```
go get github.com/pghq/go-ark
```
## Usage

A typical usage scenario:

```
import "github.com/pghq/go-ark"

// Open an in-memory data mapper
dm := ark.Open()

// Connect to the key/value store instance
conn, err := dm.ConnectKVS("inmem")
if err != nil{
    panic(err)
}

// Create a transaction
tx, err := conn.Txn(context.Background())
if err != nil{
    panic(err)
}

defer tx.Rollback()

// Commit some data
err := tx.Insert([]byte("dog"), "roof")
if err != nil{
    panic(err)
}

err := tx.InsertWithTTL([]byte("cat"), "meow")
if err != nil{
    panic(err)
}

if err := tx.Commit(); err != nil{
    panic(err)
}

// Fork a transaction: any commits and rollbacks are ignored.
_, err = conn.Txn(tx)
if err != nil{
    panic(err)
}
```

## Supported Providers
- Redis
- SQL
- KVS (in-memory)
- RDB (in-memory)