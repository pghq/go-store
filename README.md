# go-ark
Data storage for apps within the organization.

## Installation

go-ark may be installed using the go get command:

```
go get github.com/pghq/go-ark
```
## Usage

To create a new store:

```
import "github.com/pghq/go-ark"

store, err := ark.NewStore("postgres://postgres:postgres@db:5432")
if err != nil{
    panic(err)
}
```
