package gbinlog

import (
	"github.com/Shopify/sarama"
	"github.com/ml444/gbinlog/endpoints"
	"github.com/ml444/gbinlog/handler"
	"github.com/ml444/gbinlog/storage"
)


type DbConf struct {
	Addr     string
	User     string
	Password string
	Env      string
}

type Config struct {
	Db struct {
		Addr string
		User string
		Pwd  string
	}

	Handler handler.Config
}

func NewDefaultConfig() *Config {
	c := &Config{
		Db: struct {
			Addr string
			User string
			Pwd  string
		}{},
		Handler: handler.Config{
			PosStorage:     storage.PosStorageConfig{
				StorageType: 0,
				FileConfig: struct {
					Filepath string
				}{},
				RedisConfig: struct {
					Uri string
				}{},
			},
			SerializerType: 0,
			Endpoint:       endpoints.Config{},
		},
	}
	c.Handler.Endpoint.Kafka.Addrs = []string{"127.0.0.1:9092"}
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return sarama.NewManualPartitioner(topic)	
	}
	c.Handler.Endpoint.Kafka.Cfg = endpoints.KafkaCfg(*kafkaCfg)

	return c
}
