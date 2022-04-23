package handler

import (
	"errors"
	"github.com/gogo/protobuf/proto"
	"reflect"
)

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
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

type ArrayData struct {
	Columns      []string      `json:"columns"`
	BeforeValues []interface{} `json:"before_values"`
	Values       []interface{} `json:"values"`
}

type UpdateData struct {
	BeforeValues interface{} `json:"before_values"`
	AfterValues  interface{} `json:"after_values"`
}

//func ProduceMsg(topic string, partition int32, bgEvent *BinlogEvent, endpoint *endpoints.KafkaEndPoint) error {
//	bMsg, err := json.Marshal(bgEvent)
//	if err != nil {
//		log.Errorf("Err: %v \n", err)
//		return err
//	}
//
//	err = endpoint.ProduceMsg(topic, partition, bMsg)
//	if err != nil {
//		log.Errorf("Err: %v \n", err)
//		return err
//	}
//	return nil
//}

func Slice2Struct(vList []interface{}, pb proto.Message) error {
	pbV := reflect.ValueOf(pb)
	if pbV.Kind() == reflect.Ptr {
		pbV = pbV.Elem()
	}
	if pbV.Kind() != reflect.Struct {
		return errors.New("must struct")
	}
	vLen := len(vList)
	for i := 0; i < pbV.NumField(); i++ {
		if i >= vLen {
			break
		}
		value := vList[i]
		if reflect.ValueOf(value).Kind() == pbV.Field(i).Kind() {
			pbV.Field(i).Set(reflect.ValueOf(value))
		}
	}
	return nil
}

func GetHashValue(row []interface{}, fields []string, objFieldName string) (hashValue uint64) {
	for i, field := range fields {
		if field == objFieldName {
			hashValue = row[i].(uint64)
		}
	}
	return
}
