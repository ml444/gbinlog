package serializer

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"testing"
)

type User struct {
	Name    string `json:"name"`
	Gender  int8 `json:"gender"`
	Age     int16 `json:"age"`
	Size32  int32 `json:"size_32"`
	Size64  int64 `json:"size_64"`
	SizeU8  uint8 `json:"size_u_8"`
	SizeU16 uint16 `json:"size_u_16"`
	SizeU32 uint32 `json:"size_u_32"`
	SizeU64 uint64 `json:"size_u_64"`
}
// BenchmarkParser_MappingData-16    	  731450	      1529 ns/op
func BenchmarkParser_MappingData(b *testing.B) {
	user := &User{}
	parser := &JsonSerializer{}
	e := &canal.RowsEvent{
		Table: &schema.Table{
			Schema: "",
			Name:   "user",
			Columns: []schema.TableColumn{
				{Name: "name", Type: 5},
				{Name: "gender", Type: 1},
				{Name: "age", Type: 1},
				{Name: "size_32", Type: 1},
				{Name: "size_64", Type: 1},
				{Name: "size_u_8", Type: 1},
				{Name: "size_u_16", Type: 1},
				{Name: "size_u_32", Type: 1},
				{Name: "size_u_64", Type: 1},
			},
			Indexes:         nil,
			PKColumns:       nil,
			UnsignedColumns: nil,
		},
		Action: "insert",
		Rows:   [][]interface{}{{"name", 1, 20, 2000, 20000, 4, 40, 4000, 40000}},
		Header: nil,
	}
	for i := 0; i < b.N; i++ {
		_ = parser.MappingData(user, e, 0)
	}
}


func BenchmarkParser_MappingData2(b *testing.B) {
	user := &User{}
	parser := &JsonSerializer{}
	e := &canal.RowsEvent{
		Table: &schema.Table{
			Schema: "",
			Name:   "user",
			Columns: []schema.TableColumn{
				{Name: "name", Type: 5},
				{Name: "gender", Type: 1},
				{Name: "age", Type: 1},
				{Name: "size_32", Type: 1},
				{Name: "size_64", Type: 1},
				{Name: "size_u_8", Type: 1},
				{Name: "size_u_16", Type: 1},
				{Name: "size_u_32", Type: 1},
				{Name: "size_u_64", Type: 1},
			},
			Indexes:         nil,
			PKColumns:       nil,
			UnsignedColumns: nil,
		},
		Action: "insert",
		Rows:   [][]interface{}{{"name", 1, 20, 2000, 20000, 4, 40, 4000, 40000}},
		Header: nil,
	}
	for i := 0; i < b.N; i++ {
		_ = parser.MappingData(user, e, 0)
	}
}
