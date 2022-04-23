package endpoints

import "github.com/ml444/gbinlog/handler"

type IEndpoint interface {
	Send(data *handler.BinlogEvent) error
	Close() error
}

const (
	EndpointTypeKafka = 1
	EndpointTypeES    = 2
)

func NewEndPoint(cfg Config) (IEndpoint, error) {
	switch cfg.EndpointType {
	case EndpointTypeKafka:
		return NewKafkaEndpoint(cfg.Kafka)
	case EndpointTypeES:
		return NewEsEndpoint(cfg.Elasticsearch)
	}
	return nil, nil
}