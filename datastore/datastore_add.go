package datastore

import (
	"reflect"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Add adds item to the repository
func (ctx *Context) Add(collection string, data interface{}) error {
	item, err := ctx.repo.item(data)
	if err != nil {
		return errors.Wrap(err)
	}

	command := ctx.repo.client.Add().To(collection).Item(item)
	if _, err := ctx.tx.Execute(command); err != nil {
		return errors.Wrap(err)
	}

	return nil
}

// item converts a struct to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
func (r *Repository) item(in interface{}) (map[string]interface{}, error) {
	if v, ok := in.(map[string]interface{}); ok {
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errors.Newf("item of type %T is not a struct", v)
	}

	item := make(map[string]interface{})
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if key := field.Tag.Get(r.tag); key != "" {
			item[key] = v.Field(i).Interface()
		}
	}

	return item, nil
}
