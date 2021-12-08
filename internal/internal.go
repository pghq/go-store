package internal

import (
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/pghq/go-tea"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// Clock is an interface representing the internal clock time
type Clock interface {
	Now() time.Time
	Start() time.Time
	From(now func() time.Time) Clock
}

// ToSnakeCase converts a string to snake_case
// https://gist.github.com/stoewer/fbe273b711e6a06315d19552dd4d33e6
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// Fields gets all fields for datastore item(s)
func Fields(args ...interface{}) []string {
	var fields []string
	for _, arg := range args {
		switch v := arg.(type) {
		case []string:
			if len(v) > 0 {
				return v
			}
		case string:
			fields = append(fields, v)
		default:
			if i, err := ToMap(v, true); err == nil {
				for field, _ := range i {
					fields = append(fields, field)
				}
			}
		}
	}

	return fields
}

// ToMap converts a struct (w. optional tags) to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
// meant to be used for data persistence.
func ToMap(in interface{}, transient ...interface{}) (map[string]interface{}, error) {
	if v, ok := in.(map[string]interface{}); ok {
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, tea.NewErrorf("item of type %T is not a struct", v)
	}

	item := make(map[string]interface{})
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		key := sf.Tag.Get("db")
		if key == "" {
			key = sf.Name
		}

		if key == "-" || len(transient) == 0 && strings.HasSuffix(key, ",transient") {
			continue
		}

		item[strings.Split(key, ",")[0]] = v.Field(i).Interface()
	}

	return item, nil
}
