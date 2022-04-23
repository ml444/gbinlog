package serializer


const (
	ArraySerializerType = 1
	JsonSerializerType  = 2
)
type ISerializer interface {

}

func NewSerializer(serializerType int) ISerializer {
	switch serializerType {
	case ArraySerializerType:
		return NewArraySerializer()
	case JsonSerializerType:
		return NewJsonSerializer()
	default:
		panic("serializer type is error")
	}
}