package endpoints

import (
	"encoding/json"
	"testing"
	"time"
)


type BinlogEvent struct {
	Schema    string `protobuf:"bytes,1,opt,name=schema" json:"schema,omitempty"`
	Table     string `protobuf:"bytes,2,opt,name=table" json:"table,omitempty"`
	Action    string `protobuf:"bytes,3,opt,name=action" json:"action,omitempty"`
	Timestamp uint32 `protobuf:"varint,4,opt,name=datetime" json:"datetime,omitempty"`
	//Pos       uint32      `protobuf:"varint,5,opt,name=pos" json:"pos,omitempty"`
	//EndLogPod uint64      `protobuf:"varint,6,opt,name=end_log_pod,json=endLogPod" json:"end_log_pod,omitempty"`
	Data interface{} `json:"data"`
}

type ExtContactFollow struct {
	Remark string      `json:"remark" bson:"remark"`
	Info   interface{} `json:"-" bson:"-"`
}
func TestKafkaEndPoint_ProduceMsg(t *testing.T) {
	bgEvent := &BinlogEvent{
		Schema:    "biz",
		Table:     "quan_ext_contact_follow",
		Action:    "insert",
		Timestamp: 123456789,
		//Data:      nil,
	}

	b, err := json.Marshal(bgEvent)
	if err != nil {
		t.Error(err)
	}
	type args struct {
		topic     string
		partition int32
		msgData   []byte
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
	}{
		// TODO: Add test cases.
		{
			name:    "test_1",
			args:    args{
				topic:     "binlogBizTag1",
				partition: 0,
				msgData:   b,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := DefaultEndpoint
			err := p.ProduceMsg(tt.args.topic, tt.args.partition, tt.args.msgData)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProduceMsg() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10*time.Second)

		})
	}
}
