package handler

import (
	"github.com/gogo/protobuf/proto"
	"testing"
)

func TestSlice2Struct(t *testing.T) {
	type args struct {
		vList []interface{}
		pb    proto.Message
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "test", args:args{
			vList: []interface{}{uint64(123), 0, 0, uint32(2),3,uint64(4),5},
		}, wantErr:true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Slice2Struct(tt.args.vList, tt.args.pb); (err != nil) != tt.wantErr {
				t.Log(tt.args.pb.String())
				t.Errorf("Slice2Struct() error = %v, wantErr %v", err, tt.wantErr)
			}
			t.Log(tt.args.pb)
		})
	}
}
