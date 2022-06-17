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
    
    "github.com/pghq/go-store"
)

// Create an instance
db, err := store.New()
if err != nil{
    panic(err)
}

// Add some data
err := db.Do(context.TODO(), func(tx store.Txn) error {
    return tx.Add("tests", map[string]interface{}{"id": "1234"})
})

if err != nil{
    panic(err)
}
```