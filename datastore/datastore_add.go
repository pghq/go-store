package datastore

import (
	"context"
	"reflect"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

// Add adds items(s) to the repository
func (r *Repository) Add(ctx context.Context, collection string, data interface{}) error {
	items, err := r.items(data)
	if err != nil{
		return errors.Wrap(err)
	}

	tx, err := r.client.Transaction(ctx)
	if err != nil {
		return errors.Wrap(err)
	}
	defer tx.Rollback()

	for _, item := range items {
		command := r.client.Add().To(collection).Item(item)
		if _, err := tx.Execute(command); err != nil {
			return errors.Wrap(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err)
	}

	return nil
}

// items converts an interface to a slice of maps using reflection
func (r *Repository) items(in interface{}) ([]map[string]interface{}, error) {
	if in == nil{
		return nil, errors.New("nil value passed")
	}

	if v, ok := in.([]map[string]interface{}); ok{
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Slice{
		item, err := r.item(v.Interface())
		if err != nil{
			return nil, errors.Wrap(err)
		}
		return []map[string]interface{}{item}, nil
	}

	if v.IsNil() || v.Len() == 0 {
		return nil, errors.Newf("bad repository value of type %T", v)
	}

	res := make([]map[string]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		item, err := r.item(v.Index(i).Interface())
		if err != nil{
			return nil, errors.Wrap(err)
		}
		res[i] = item
	}

	return res, nil
}

// item converts a struct to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
func (r *Repository) item(in interface{}) (map[string]interface{}, error){
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
