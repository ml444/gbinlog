package serializer

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/json-iterator/go"
	"reflect"
	"strings"
	"time"
)

type JsonSerializer struct{}

func NewJsonSerializer() *JsonSerializer {
	return &JsonSerializer{}
}

func (m *JsonSerializer) MappingData(pd interface{}, e *canal.RowsEvent, n int) error {
	v := reflect.ValueOf(pd)
	s := reflect.Indirect(v)
	t := s.Type()
	num := t.NumField()
	for k := 0; k < num; k++ {
		columnName := getColumnName4StructTag(t.Field(k).Tag)
		if columnName == "" {
			continue
		}

		switch s.Field(k).Type().Kind() {
		case reflect.Bool:
			s.Field(k).SetBool(m.parseBool(e, n, columnName))
		case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32, reflect.Int64:
			s.Field(k).SetInt(m.parseInt(e, n, columnName))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			s.Field(k).SetUint(m.parseUint(e, n, columnName))
		case reflect.String:
			s.Field(k).SetString(m.parseString(e, n, columnName))
		case reflect.Float64, reflect.Float32:
			s.Field(k).SetFloat(m.parseFloat(e, n, columnName))
		case reflect.Struct:
			newObject := reflect.New(s.Field(k).Type()).Interface()
			json := m.parseString(e, n, columnName)

			err := jsoniter.Unmarshal([]byte(json), &newObject)
			if err != nil {
				return err
			}

			s.Field(k).Set(reflect.ValueOf(newObject).Elem().Convert(s.Field(k).Type()))
		//case reflect.Array,reflect.Slice:
		//	s.Field(k).Set()
		//case "Time":
		//	timeVal := m.parseDateTime(e, n, columnName)
		//	s.Field(k).Set(reflect.ValueOf(timeVal))
		default:
				newObject := reflect.New(s.Field(k).Type()).Interface()
				json := m.parseString(e, n, columnName)

				err := jsoniter.Unmarshal([]byte(json), &newObject)
				if err != nil {
					return err
				}

				s.Field(k).Set(reflect.ValueOf(newObject).Elem().Convert(s.Field(k).Type()))

		}
	}
	return nil
}


func (m *JsonSerializer) parseDateTime(e *canal.RowsEvent, n int, columnName string) time.Time {
	columnIdx := m.getColumnIdxByName(e, columnName)
	if e.Table.Columns[columnIdx].Type != schema.TYPE_TIMESTAMP {
		panic("Not dateTime type")
	}
	t, _ := time.Parse("2006-01-02 15:04:05", e.Rows[n][columnIdx].(string))

	return t
}

func (m *JsonSerializer) parseInt(e *canal.RowsEvent, n int, columnName string) int64 {
	columnIdx := m.getColumnIdxByName(e, columnName)
	if e.Table.Columns[columnIdx].Type != schema.TYPE_NUMBER {
		return 0
	}

	switch e.Rows[n][columnIdx].(type) {
	case int8:
		return int64(e.Rows[n][columnIdx].(int8))
	case int16:
		return int64(e.Rows[n][columnIdx].(int16))
	case int32:
		return int64(e.Rows[n][columnIdx].(int32))
	case int64:
		return e.Rows[n][columnIdx].(int64)
	case int:
		return int64(e.Rows[n][columnIdx].(int))
	}
	return 0
}


func (m *JsonSerializer) parseUint(e *canal.RowsEvent, n int, columnName string) uint64 {
	columnIdx := m.getColumnIdxByName(e, columnName)
	if e.Table.Columns[columnIdx].Type != schema.TYPE_NUMBER {
		return 0
	}

	switch e.Rows[n][columnIdx].(type) {
	case uint8:
		return uint64(e.Rows[n][columnIdx].(uint8))
	case uint16:
		return uint64(e.Rows[n][columnIdx].(uint16))
	case uint32:
		return uint64(e.Rows[n][columnIdx].(uint32))
	case uint64:
		return e.Rows[n][columnIdx].(uint64)
	case uint:
		return uint64(e.Rows[n][columnIdx].(uint))
	}
	return 0
}


func (m *JsonSerializer) parseFloat(e *canal.RowsEvent, n int, columnName string) float64 {

	columnIdx := m.getColumnIdxByName(e, columnName)
	if e.Table.Columns[columnIdx].Type != schema.TYPE_FLOAT {
		panic("Not float type")
	}

	switch e.Rows[n][columnIdx].(type) {
	case float32:
		return float64(e.Rows[n][columnIdx].(float32))
	case float64:
		return e.Rows[n][columnIdx].(float64)
	}
	return float64(0)
}

func (m *JsonSerializer) parseBool(e *canal.RowsEvent, n int, columnName string) bool {

	val := m.parseInt(e, n, columnName)
	if val == 1 {
		return true
	}
	return false
}

func (m *JsonSerializer) parseString(e *canal.RowsEvent, n int, columnName string) string {

	columnIdx := m.getColumnIdxByName(e, columnName)
	if e.Table.Columns[columnIdx].Type == schema.TYPE_ENUM {

		values := e.Table.Columns[columnIdx].EnumValues
		if len(values) == 0 {
			return ""
		}
		if e.Rows[n][columnIdx] == nil {
			//Если в енум лежит нуул ставим пустую строку
			return ""
		}

		return values[e.Rows[n][columnIdx].(int64)-1]
	}

	value := e.Rows[n][columnIdx]

	switch value := value.(type) {
	case []byte:
		return string(value)
	case string:
		return value
	}
	return ""
}

func (m *JsonSerializer) getColumnIdxByName(e *canal.RowsEvent, name string) int {
	for id, value := range e.Table.Columns {
		if value.Name == name {
			return id
		}
	}
	panic(fmt.Sprintf("There is no column %s in table %s.%s", name, e.Table.Schema, e.Table.Name))
}

func getColumnName4StructTag(tags reflect.StructTag) string {
	for _, str := range []string{tags.Get("json")} {
		values := strings.Split(str, ",")
		if len(values) > 0 {
			return values[0]
		}
	}
	return ""
}