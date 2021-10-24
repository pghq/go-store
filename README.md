# go-datastore
Data storage for apps within the organization.

## Installation

go-datastore may be installed using the go get command:

```
go get github.com/pghq/go-datastore
```
## Usage

```
import (
    "github.com/pghq/go-datastore/datastore/postgres"
    "github.com/pghq/go-datastore/datastore"
)
```

To create a new repo:

```
repo, err := datastore.New(postgres.New("postgres://postgres:postgres@db:5432"))
if err != nil{
    panic(err)
}

// TODO: See tests for specific use cases...
```
